package subsetter

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type Sync struct {
	source      *pgxpool.Pool
	destination *pgxpool.Pool
	fraction    float64
	verbose     bool
	force       []Force
}

type Force struct {
	Table string
	Where string
}

func NewSync(source string, target string, fraction float64, force []Force, verbose bool) (*Sync, error) {
	src, err := pgxpool.New(context.Background(), source)
	if err != nil {
		return nil, err
	}

	err = src.Ping(context.Background())
	if err != nil {
		return nil, err
	}

	dst, err := pgxpool.New(context.Background(), source)
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
		force:       force,
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
		return
	}
	if err = CopyStringToTable(table.Name, data, destination); err != nil {
		return
	}
	return
}

// ViableSubset returns a subset of tables that can be copied to the destination database
func ViableSubset(tables []Table) (subset []Table) {

	// Filter out tables with no rows
	subset = lo.Filter(tables, func(table Table, _ int) bool { return table.Rows > 0 })

	// Get all relations
	relationsR := lo.FlatMap(subset, func(table Table, _ int) []Relation { return table.Relations })
	relations := lo.Map(relationsR, func(relation Relation, _ int) string { return relation.ForeignTable })

	// Filter out tables that are relations of other tables
	// they will be copied later
	subset = lo.Filter(subset, func(table Table, _ int) bool {
		return !lo.Contains(relations, table.Name)
	})

	return
}

// CopyTables copies the data from a list of tables in the source database to the destination database
func (s *Sync) CopyTables(tables []Table) (err error) {
	for _, table := range tables {
		log.Info().Msgf("Copying table %s", table.Name)
		if err = copyTableData(table, s.source, s.destination); err != nil {
			return
		}

		for _, force := range s.force {
			if force.Table == table.Name {
				log.Info().Msgf("Selecting forced rows for table %s", table.Name)
				var data string
				if data, err = CopyQueryToString(force.Where, s.source); err != nil {
					return
				}
				if err = CopyStringToTable(table.Name, data, s.destination); err != nil {
					return
				}
			}
		}

		for _, relation := range table.Relations {
			// Backtrace the inserted ids from main table to related table

			log.Info().Msgf("Copying relation %s for table %s", relation, table.Name)
			var data string
			if data, err = CopyQueryToString(relation.Query(), s.source); err != nil {
				return
			}
			if err = CopyStringToTable(table.Name, data, s.destination); err != nil {
				return
			}
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
			log.Info().Msgf("Copying table %s with %d rows", t.Name, t.Rows)
			log.Info().Msgf("Relations: %v", t.Relations)

		}
	}

	if err = s.CopyTables(subset); err != nil {
		return
	}

	return
}
