package subsetter

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type SyncError struct {
	Retry bool
}

func (se *SyncError) Error() string {
	return fmt.Sprintf("Sync error: retry=%t", se.Retry)
}

type Sync struct {
	source      *pgxpool.Pool
	destination *pgxpool.Pool
	fraction    float64
	verbose     bool
	include     []Rule
	exclude     []Rule
}

type Rule struct {
	Table string
	Where string
}

func (r *Rule) String() string {
	return fmt.Sprintf("%s:%s", r.Table, r.Where)
}

func (r *Rule) Query(exclude []string) string {
	if r.Where == "" {
		return fmt.Sprintf("SELECT * FROM %s", r.Table)
	}

	if len(exclude) > 0 {
		exclude = lo.Map(exclude, func(s string, _ int) string {
			return QuoteString(s)
		})
		r.Where = fmt.Sprintf("%s AND id NOT IN (%s)", r.Where, strings.Join(exclude, ","))
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", r.Table, r.Where)
}

func (r *Rule) Copy(s *Sync) (err error) {
	log.Debug().Str("query", r.Where).Msgf("Transferring forced rows for table %s", r.Table)
	var data string

	keyName, err := GetPrimaryKeyName(r.Table, s.destination)
	if err != nil {
		return errors.Wrapf(err, "Error getting primary key for table %s", r.Table)
	}

	q := fmt.Sprintf(`SELECT %s FROM %s`, keyName, r.Table)
	log.Debug().Str("query", q).Msgf("Getting keys for %s from target", r.Table)

	excludedIDs := []string{}
	if primaryKeys, err := GetKeys(q, s.destination); err == nil {
		excludedIDs = primaryKeys
	}
	log.Debug().Strs("excludedIDs", excludedIDs).Msgf("Excluded IDs for table %s", r.Table)

	if data, err = CopyQueryToString(r.Query(excludedIDs), s.source); err != nil {
		return errors.Wrapf(err, "Error copying forced rows for table %s", r.Table)
	}
	if err = CopyStringToTable(r.Table, data, s.destination); err != nil {
		return errors.Wrapf(err, "Error inserting forced rows for table %s", r.Table)
	}
	return
}

func NewSync(source string, target string, fraction float64, include []Rule, exclude []Rule, verbose bool) (*Sync, error) {
	src, err := pgxpool.New(context.Background(), source)
	if err != nil {
		return nil, err
	}

	err = src.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	dst, err := pgxpool.New(context.Background(), target)
	if err != nil {
		return nil, err
	}
	err = dst.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	return &Sync{
		source:      src,
		destination: dst,
		fraction:    fraction,
		verbose:     verbose,
		include:     include,
		exclude:     exclude,
	}, nil
}

// Close closes the connections to the source and destination databases
func (s *Sync) Close() {
	s.source.Close()
	s.destination.Close()
}

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
			if err = RelationalCopy(depth, tables, missingTable, visitedTables, source, destination); err != nil {
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

func RelationalCopy(
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
			log.Debug().Str("table", relatedTable.Name).Strs("relatedQueries", relatedQueries).Msg("Transferring with RelationalCopy")
		}

		if err = copyTableData(relatedTable, relatedQueries, false, source, destination); err != nil {
			if condition, ok := err.(*pgconn.PgError); ok && condition.Code == "23503" { // foreign key violation
				if err := RelationalCopy(depth, tables, relatedTable, visitedTables, source, destination); err != nil {
					return errors.Wrapf(err, "Error copying table %s", relatedTable.Name)
				}
			}
			return errors.Wrapf(err, "Error copying table %s", relatedTable.Name)
		}

	}

	return nil
}

// CopyTables copies the data from a list of tables in the source database to the destination database
func (s *Sync) CopyTables(tables []Table) (err error) {

	visitedTables := []string{}
	// Copy tables without relations first
	for _, table := range lo.Filter(tables, func(table Table, _ int) bool {
		return len(table.Relations) == 0
	}) {
		log.Info().Str("table", table.Name).Msg("Transferring")
		if err = copyTableData(table, []string{}, true, s.source, s.destination); err != nil {
			return errors.Wrapf(err, "Error copying table %s", table.Name)
		}

		for _, include := range s.include {
			if include.Table == table.Name {
				err = include.Copy(s)
				if err != nil {
					return errors.Wrapf(err, "Error copying forced rows for table %s", table.Name)
				}
			}
		}

		visitedTables = append(visitedTables, table.Name)
	}

	// Prevent infinite loop, by setting max depth
	depth := 0
	// Copy tables with relations
	maybeRetry := []Table{}

	for _, complexTable := range lo.Filter(tables, func(table Table, _ int) bool {
		return len(table.Relations) > 0
	}) {
		log.Info().Str("table", complexTable.Name).Msg("Transferring")
		if err := RelationalCopy(&depth, tables, complexTable, &visitedTables, s.source, s.destination); err != nil {
			log.Info().Str("table", complexTable.Name).Msgf("Transferring failed, retrying later")
			maybeRetry = append(maybeRetry, complexTable)
		}

		for _, include := range s.include {
			if include.Table == complexTable.Name {
				log.Warn().Str("table", complexTable.Name).Msgf("Transferring forced rows for relational table is not supported.")
			}
		}
	}

	// Retry tables with relations
	visitedRetriedTables := []string{}
	for _, retiredTable := range maybeRetry {
		log.Info().Str("table", retiredTable.Name).Msg("Transferring")
		if err := RelationalCopy(&depth, tables, retiredTable, &visitedRetriedTables, s.source, s.destination); err != nil {
			log.Warn().Str("table", retiredTable.Name).Msgf("Transferring failed, try increasing fraction percentage")
		}
	}

	// Remove excluded rows and print reports
	fmt.Println()
	fmt.Println("Report:")
	for _, table := range tables {
		// to ensure no data is in excluded tables
		for _, exclude := range s.exclude {
			if exclude.Table == table.Name {
				log.Info().Str("query", exclude.Where).Msgf("Deleting excluded rows for table %s", table.Name)
				if err = DeleteRows(exclude.Table, exclude.Where, s.destination); err != nil {
					return errors.Wrapf(err, "Error deleting excluded rows for table %s", table.Name)
				}
			}
		}

		count, _ := CountRows(table.Name, s.destination)
		log.Info().Int("count", count).Msgf("Copied table %s", table.Name)
	}

	return
}

// Sync copies a subset of tables from source to destination
func (s *Sync) Sync() (err error) {
	var tables []Table

	// Get all tables with rows
	if tables, err = GetTablesWithRows(s.source); err != nil {
		return
	}

	// Filter out tables that are not in the include list
	ruleExcludedTables := lo.Map(s.exclude, func(rule Rule, _ int) string {
		return rule.Table
	})
	tables = lo.Filter(tables, func(table Table, _ int) bool {
		return !lo.Contains(ruleExcludedTables, table.Name) // excluded tables
	})

	// Calculate fraction to be coped over
	if tables = GetTargetSet(s.fraction, tables); err != nil {
		return
	}

	if s.verbose {
		log.Info().Strs("tables", lo.Map(tables, func(table Table, _ int) string {
			return table.Name
		})).Msg("Tables to be copied")
	}

	// Copy tables
	if err = s.CopyTables(tables); err != nil {
		return
	}

	return
}
