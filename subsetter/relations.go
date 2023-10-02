package subsetter

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

var cachedRelations *[]RelationRaw
var mutexCachedRelations sync.Once

type Relation struct {
	PrimaryTable  string
	PrimaryColumn string
	ForeignTable  string
	ForeignColumn string
}

func (r *Relation) IsSelfRelated() bool {
	return r.PrimaryTable == r.ForeignTable
}

func (r *Relation) Query(subset []string) string {

	subset = lo.Map(subset, func(s string, _ int) string {
		return QuoteString(s)
	})

	return fmt.Sprintf(`SELECT * FROM %s WHERE %s IN (%s)`, r.PrimaryTable, r.PrimaryColumn, strings.Join(subset, ","))
}

func (r *Relation) PrimaryQuery() string {
	return fmt.Sprintf(`SELECT %s FROM %s`, r.ForeignColumn, r.ForeignTable)
}

// RelationRaw is a raw representation of a relation in the database.
type RelationRaw struct {
	PrimaryTable string
	ForeignTable string
	SQL          string
}

// toRelation converts a RelationRaw to a Relation.
func (r *RelationRaw) toRelation() Relation {
	var rel Relation
	re := regexp.MustCompile(`FOREIGN KEY \((\w+)\) REFERENCES (\w+)\((\w+)\).*`)
	matches := re.FindStringSubmatch(r.SQL)
	if len(matches) == 4 {
		rel.PrimaryColumn = matches[1]
		rel.ForeignTable = matches[2]
		rel.ForeignColumn = matches[3]
	}
	rel.PrimaryTable = r.PrimaryTable
	return rel
}

func getAllRelations(table string, conn *pgxpool.Pool) *[]RelationRaw {

	mutexCachedRelations.Do(func() {
		q := `SELECT
		conrelid::regclass AS primary_table,
		confrelid::regclass AS referenced_table,
		pg_get_constraintdef(c.oid, TRUE) AS sql
	FROM
		pg_constraint c
		JOIN pg_namespace n ON n.oid = c.connamespace
	WHERE
		c.contype = 'f'
		AND n.nspname = 'public';`

		rows, err := conn.Query(context.Background(), q)
		if err != nil {
			return
		}
		defer rows.Close()
		relations := []RelationRaw{}
		for rows.Next() {
			var rel RelationRaw

			err = rows.Scan(&rel.PrimaryTable, &rel.ForeignTable, &rel.SQL)
			if err != nil {
				return
			}
			relations = append(relations, rel)
			log.Debug().Str("table", rel.PrimaryTable).Str("foreign", rel.ForeignTable).Msg("Found relation")
		}
		cachedRelations = &relations

	})

	return cachedRelations
}

// GetRelations returns a list of tables that are foreign key for particular table.
func GetRelations(table string, conn *pgxpool.Pool) (relations []Relation) {
	for _, rel := range *getAllRelations(table, conn) {
		if table == rel.PrimaryTable {
			relations = append(relations, rel.toRelation())
		}
	}
	return
}

// GetRequiredBy returns a list of tables that have are foreign key for particular table.
func GetRequiredBy(table string, conn *pgxpool.Pool) (relations []Relation) {
	for _, rel := range *getAllRelations(table, conn) {
		if table == rel.ForeignTable {
			relations = append(relations, rel.toRelation())
		}
	}
	return
}
