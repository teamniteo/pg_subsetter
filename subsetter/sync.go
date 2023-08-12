package subsetter

import (
	"context"
	"sort"

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
func copyTableData(table Table, source *pgxpool.Pool, destination *pgxpool.Pool) (err error) {
	var data string
	if data, err = CopyTableToString(table.Name, table.Rows, source); err != nil {
		log.Error().Err(err).Msgf("Error copying table %s", table.Name)
		return
	}
	if err = CopyStringToTable(table.Name, data, destination); err != nil {
		log.Error().Err(err).Msgf("Error pasting table %s", table.Name)
		return
	}
	return
}

// ViableSubset returns a subset of tables that can be copied to the destination database
func ViableSubset(tables []Table) (subset []Table) {

	// Filter out tables with no rows
	subset = lo.Filter(tables, func(table Table, _ int) bool { return table.Rows > 0 })

	// Ignore tables with relations to tables
	// they are populated by the primary table
	tablesWithRelations := lo.Filter(tables, func(table Table, _ int) bool {
		return len(table.Relations) > 0
	})

	var excludedTables []string
	for _, table := range tablesWithRelations {
		for _, relation := range table.Relations {
			if table.Name != relation.PrimaryTable {
				excludedTables = append(excludedTables, relation.PrimaryTable)
			}
		}
	}

	subset = lo.Filter(subset, func(table Table, _ int) bool {
		return !lo.Contains(excludedTables, table.Name)
	})

	sort.Slice(subset, func(i, j int) bool {
		return len(subset[i].Relations) < len(subset[j].Relations)
	})
	return
}

// CopyTables copies the data from a list of tables in the source database to the destination database
func (s *Sync) CopyTables(tables []Table) (err error) {

	for _, table := range tables {

		if err = copyTableData(table, s.source, s.destination); err != nil {
			return errors.Wrapf(err, "Error copying table %s", table.Name)
		}

		for _, include := range s.include {
			if include.Table == table.Name {
				log.Info().Str("query", include.Where).Msgf("Selecting forced rows for table %s", table.Name)
				var data string
				if data, err = CopyQueryToString(include.Where, s.source); err != nil {
					return errors.Wrapf(err, "Error copying forced rows for table %s", table.Name)
				}
				if err = CopyStringToTable(table.Name, data, s.destination); err != nil {
					return errors.Wrapf(err, "Error inserting forced rows for table %s", table.Name)
				}
			}
		}

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

		for _, relation := range table.Relations {
			// Backtrace the inserted ids from main table to related table
			var pKeys []string
			if pKeys, err = GetKeys(relation.PrimaryQuery(), s.destination); err != nil {
				return errors.Wrapf(err, "Error getting primary keys for %s", relation.PrimaryTable)
			}
			var data string
			if data, err = CopyQueryToString(relation.Query(pKeys), s.source); err != nil {
				return errors.Wrapf(err, "Error copying related table %s", relation.PrimaryTable)
			}
			if err = CopyStringToTable(relation.PrimaryTable, data, s.destination); err != nil {
				if condition, ok := err.(*pgconn.PgError); ok && condition.Code == "23503" {
					log.Warn().Msgf("Skipping %s because of foreign key violation", relation.PrimaryTable)
					err = nil
				} else {
					return errors.Wrapf(err, "Error inserting related table %s", relation.PrimaryTable)
				}

			}
			count, _ := CountRows(relation.PrimaryTable, s.destination)
			log.Info().Int("count", count).Msgf("Copied %s for %s", relation.PrimaryTable, table.Name)
		}
	}
	return
}

// Sync copies a subset of tables from source to destination
func (s *Sync) Sync() (err error) {
	var tables []Table
	if tables, err = GetTablesWithRows(s.source); err != nil {
		return
	}

	var allTables []Table
	if allTables = GetTargetSet(s.fraction, tables); err != nil {
		return
	}

	subset := ViableSubset(allTables)

	if s.verbose {
		for _, t := range subset {
			log.Info().
				Str("table", t.Name).
				Int("rows", t.Rows).
				Str("related", t.RelationNames()).
				Msg("Prepared for sync")

		}
	}

	if err = s.CopyTables(subset); err != nil {
		return
	}

	return
}
