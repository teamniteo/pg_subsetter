package subsetter

import (
	"context"
	"fmt"

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

// CopyTables copies the data from a list of tables in the source database to the destination database
func (s *Sync) CopyTables(tables []Table) (err error) {

	// Filter out tables that are in include list and have custom rule
	customRuleTables := lo.Uniq(lo.Map(s.include, func(rule Rule, _ int) string {
		return rule.Table
	}))

	visitedTables := []string{}

	// Copy tables without relations first
	for _, table := range lo.Filter(tables, func(table Table, _ int) bool {
		return len(table.Relations) == 0
	}) {
		log.Info().Str("table", table.Name).Msg("Transferring")
		if !lo.Contains(customRuleTables, table.Name) {
			if err = copyTableData(table, []string{}, true, s.source, s.destination); err != nil {
				return errors.Wrapf(err, "Error copying table %s", table.Name)
			}
		} else {
			for _, include := range s.include {
				if include.Table == table.Name {
					err = include.Copy(s)
					if err != nil {
						return errors.Wrapf(err, "Error copying forced rows for table %s", table.Name)
					}
					if include.Where != RuleAll {
						// reverse copy all related rows
						requiredTables, _ := RequiredTableGraph(table.Name, table.RequiredBy)
						for _, relation := range requiredTables {
							if relation == table.Name { // skip self
								continue
							}
							relatedTable := TableByName(tables, relation)
							if relatedTable.Name == "" { // skip unresolvable tables
								continue
							}
							err := include.CopyRelated(s, relatedTable)
							if err != nil {
								log.Warn().Str("table", relatedTable.Name).Msgf("No rows found for related table")
							}
						}
					}
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
		if err := relationalCopy(&depth, tables, complexTable, &visitedTables, s.source, s.destination); err != nil {
			log.Info().Str("table", complexTable.Name).Msgf("Transferring failed, retrying later")
			maybeRetry = append(maybeRetry, complexTable)
		}

		for _, include := range s.include {
			if include.Table == complexTable.Name {
				// Copy only primary row by first setting ignore relational checks
				_, err := s.destination.Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER USER;", complexTable.Name))
				if err != nil {
					return errors.Wrap(err, "Error setting session_replication_role to replica")
				}

				err = include.Copy(s)
				if err != nil {
					return errors.Wrapf(err, "Error copying forced rows for table %s", complexTable.Name)
				}

				// Set relational checks back
				_, err = s.destination.Exec(context.Background(), fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER USER;", complexTable.Name))
				if err != nil {
					return errors.Wrap(err, "Error setting session_replication_role to origin")
				}
			}
		}
	}

	// Retry tables with relations
	visitedRetriedTables := []string{}
	for _, retiredTable := range maybeRetry {
		log.Info().Str("table", retiredTable.Name).Msg("Transferring")
		if err := relationalCopy(&depth, tables, retiredTable, &visitedRetriedTables, s.source, s.destination); err != nil {
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

	// Calculate fraction to be copied over
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
