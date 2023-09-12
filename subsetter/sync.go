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

func (r *Rule) Query() string {
	if r.Where == "" {
		return fmt.Sprintf("SELECT * FROM %s", r.Table)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", r.Table, r.Where)
}

func (r *Rule) Copy(s *Sync) (err error) {
	log.Debug().Str("query", r.Where).Msgf("Copying forced rows for table %s", r.Table)
	var data string
	if data, err = CopyQueryToString(r.Query(), s.source); err != nil {
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
	subselectQeury := ""
	if len(relatedQueries) > 0 {
		subselectQeury = "WHERE " + strings.Join(relatedQueries, " AND ")
	}

	limit := ""
	if withLimit {
		limit = fmt.Sprintf("LIMIT %d", table.Rows)
	}

	var data string
	if data, err = CopyTableToString(table.Name, limit, subselectQeury, source); err != nil {
		log.Error().Err(err).Msgf("Error getting table data for %s", table.Name)
		return
	}
	if err = CopyStringToTable(table.Name, data, destination); err != nil {
		log.Error().Err(err).Msgf("Error pushing table data for	 %s", table.Name)
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
			log.Warn().Int("depth", *depth).Msgf("No keys found for %s", relation.ForeignTable)
			missingTable := lo.Filter(tables, func(table Table, _ int) bool {
				return table.Name == relation.ForeignTable
			})[0]
			RelationalCopy(depth, tables, missingTable, visitedTables, source, destination)
			*depth++
			log.Debug().Int("depth", *depth).Msgf("Retrying keys for %s", relation.ForeignTable)
			if *depth < 1 {
				goto retry
			} else {
				log.Warn().Int("depth", *depth).Msgf("Max depth reached for %s", relation.ForeignTable)
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
		relatedTable := lo.Filter(tables, func(table Table, _ int) bool {
			return table.Name == tableName
		})[0]
		*visitedTables = append(*visitedTables, relatedTable.Name)
		// Use realized query to get priamry keys that are already in the destination for all related tables

		// Selection query for this table
		relatedQueries := []string{}

		for _, relation := range relatedTable.Relations {
			relatedQueriesBuilder(depth, tables, relation, relatedTable, source, destination, visitedTables, &relatedQueries)
		}

		if len(relatedQueries) > 0 {
			log.Debug().Str("table", relatedTable.Name).Strs("relatedQueries", relatedQueries).Msg("Copying with RelationalCopy")
		}

		if err = copyTableData(relatedTable, relatedQueries, false, source, destination); err != nil {
			if condition, ok := err.(*pgconn.PgError); ok && condition.Code == "23503" { // foreign key violation
				RelationalCopy(depth, tables, relatedTable, visitedTables, source, destination)
			}
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
		log.Info().Str("table", table.Name).Msg("Copying")
		if err = copyTableData(table, []string{}, true, s.source, s.destination); err != nil {
			return errors.Wrapf(err, "Error copying table %s", table.Name)
		}

		for _, include := range s.include {
			if include.Table == table.Name {
				include.Copy(s)
			}
		}

		visitedTables = append(visitedTables, table.Name)
	}

	// Prevent infinite loop, by setting max depth
	depth := 0
	// Copy tables with relations
	for _, complexTable := range lo.Filter(tables, func(table Table, _ int) bool {
		return len(table.Relations) > 0
	}) {
		log.Info().Str("table", complexTable.Name).Msg("Copying")
		RelationalCopy(&depth, tables, complexTable, &visitedTables, s.source, s.destination)

		for _, include := range s.include {
			if include.Table == complexTable.Name {
				log.Warn().Str("table", complexTable.Name).Msgf("Copying forced rows for relational table is not supported.")
			}
		}
	}

	// Remove excluded rows and print reports
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
