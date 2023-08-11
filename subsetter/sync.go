package subsetter

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type Sync struct {
	source      *pgx.Conn
	destination *pgx.Conn
	fraction    float64
	verbose     bool
}

func NewSync(source string, target string, fraction float64, verbose bool) (*Sync, error) {
	src, err := pgx.Connect(context.Background(), source)
	if err != nil {
		return nil, err
	}
	dst, err := pgx.Connect(context.Background(), source)
	if err != nil {
		return nil, err
	}

	return &Sync{
		source:      src,
		destination: dst,
		fraction:    fraction,
		verbose:     verbose,
	}, nil
}

func (s *Sync) Sync() (err error) {
	var tables []Table
	if tables, err = GetTablesWithRows(s.source); err != nil {
		return
	}

	var subset []Table
	if subset = GetTargetSet(s.fraction, tables); err != nil {
		return
	}
	// Filter out tables with no rows
	subset = lo.Filter(subset, func(table Table, _ int) bool { return table.Rows > 0 })

	// Generate a list of relations that should be excluded from the subset
	relations := lo.FlatMap(subset, func(table Table, _ int) []string { return table.Relations })

	// Filter out tables that are relations of other tables
	// they will be copied later
	subset = lo.Filter(subset, func(table Table, _ int) bool {
		return !lo.Contains(relations, table.Name)
	})

	for _, table := range subset {
		var data string
		if data, err = CopyTableToString(table.Name, table.Rows, s.source); err != nil {
			return
		}
		if err = CopyStringToTable(table.Name, data, s.destination); err != nil {
			return
		}
	}

	//copy relations, TODO: make this work
	for _, table := range subset {
		for _, relation := range table.Relations {
			log.Debug().Msgf("Copying relation %s for table %s", relation, table.Name)
		}
	}

	return
}
