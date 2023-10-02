package subsetter

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

// copyTableData copies the data from a table in the source database to the destination database
func copyTableData(table Table, relatedQueries []string, withLimit bool, source *pgxpool.Pool, destination *pgxpool.Pool) (err error) {
	// Backtrace the inserted ids from main table to related table
	subSelectQuery := ""
	if len(relatedQueries) > 0 {
		subSelectQuery = "WHERE " + strings.Join(relatedQueries, " AND ")
	}

	limit := ""
	if withLimit {
		limit = fmt.Sprintf("LIMIT %d", table.Rows)
	}

	var data string
	if data, err = CopyTableToString(table.Name, limit, subSelectQuery, source); err != nil {
		//log.Error().Err(err).Str("table", table.Name).Msg("Error getting table data")
		return
	}
	if err = CopyStringToTable(table.Name, data, destination); err != nil {
		//log.Error().Err(err).Str("table", table.Name).Msg("Error pushing table data")
		return
	}
	return

}

func relatedQueriesBuilder(
	depth *int,
	tables []Table,
	relation Relation,
	table Table,
	source *pgxpool.Pool,
	destination *pgxpool.Pool,
	visitedTables *[]string,
	relatedQueries *[]string,
) (err error) {

retry:
	q := fmt.Sprintf(`SELECT %s FROM %s`, relation.ForeignColumn, relation.ForeignTable)
	log.Debug().Str("query", q).Msgf("Getting keys for %s from target", table.Name)

	if primaryKeys, err := GetKeys(q, destination); err != nil {
		log.Error().Err(err).Msgf("Error getting keys for %s", table.Name)
		return err
	} else {
		if len(primaryKeys) == 0 {

			missingTable := TableByName(tables, relation.ForeignTable)
			if err = relationalCopy(depth, tables, missingTable, visitedTables, source, destination); err != nil {
				return errors.Wrapf(err, "Error copying table %s", missingTable.Name)
			}

			// Retry short circuit
			*depth++

			log.Debug().Int("depth", *depth).Msgf("Retrying keys for %s", relation.ForeignTable)
			if *depth < 1 {
				goto retry
			} else {
				log.Debug().Str("table", relation.ForeignTable).Str("primary", relation.PrimaryTable).Msgf("No keys found at this time")
				return errors.New("Max depth reached")
			}

		} else {
			*depth = 0
			keys := lo.Map(primaryKeys, func(key string, _ int) string {
				return QuoteString(key)
			})
			rq := fmt.Sprintf(`%s IN (%s)`, relation.PrimaryColumn, strings.Join(keys, ","))
			*relatedQueries = append(*relatedQueries, rq)
		}
	}
	return nil
}

func relationalCopy(
	depth *int,
	tables []Table,
	table Table,
	visitedTables *[]string,
	source *pgxpool.Pool,
	destination *pgxpool.Pool,
) error {
	log.Debug().Str("table", table.Name).Msg("Preparing")

	relatedTables, err := TableGraph(table.Name, table.Relations)
	if err != nil {
		return errors.Wrapf(err, "Error sorting tables from graph")
	}
	log.Debug().Strs("tables", relatedTables).Msgf("Order of copy")

	for _, tableName := range relatedTables {

		if lo.Contains(*visitedTables, tableName) {
			continue
		}

		relatedTable := TableByName(tables, tableName)
		*visitedTables = append(*visitedTables, relatedTable.Name)
		// Use realized query to get primary keys that are already in the destination for all related tables

		// Selection query for this table
		relatedQueries := []string{}

		for _, relation := range relatedTable.Relations {
			err := relatedQueriesBuilder(depth, tables, relation, relatedTable, source, destination, visitedTables, &relatedQueries)
			if err != nil {
				return err
			}
		}

		if len(relatedQueries) > 0 {
			log.Debug().Str("table", relatedTable.Name).Strs("relatedQueries", relatedQueries).Msg("Transferring with relationalCopy")
		}

		if err = copyTableData(relatedTable, relatedQueries, false, source, destination); err != nil {
			if condition, ok := err.(*pgconn.PgError); ok && condition.Code == "23503" { // foreign key violation
				if err := relationalCopy(depth, tables, relatedTable, visitedTables, source, destination); err != nil {
					return errors.Wrapf(err, "Error copying table %s", relatedTable.Name)
				}
			}
			return errors.Wrapf(err, "Error copying table %s", relatedTable.Name)
		}

	}

	return nil
}
