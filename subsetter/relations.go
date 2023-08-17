package subsetter

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
)

type Relation struct {
	PrimaryTable  string
	PrimaryColumn string
	ForeignTable  string
	ForeignColumn string
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

type RelationInfo struct {
	TableName    string
	ForeignTable string
	SQL          string
}

func (r *RelationInfo) toRelation() Relation {
	var rel Relation
	re := regexp.MustCompile(`FOREIGN KEY \((\w+)\) REFERENCES (\w+)\((\w+)\).*`)
	matches := re.FindStringSubmatch(r.SQL)
	if len(matches) == 4 {
		rel.PrimaryColumn = matches[1]
		rel.ForeignTable = matches[2]
		rel.ForeignColumn = matches[3]
	}
	rel.PrimaryTable = r.TableName
	return rel
}

// GetRelations returns a list of tables that have a foreign key for particular table.
func GetRelations(table string, conn *pgxpool.Pool) (relations []Relation, err error) {

	q := `SELECT
		conrelid::regclass AS table_name,
		confrelid::regclass AS refrerenced_table,
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

	for rows.Next() {
		var rel RelationInfo

		err = rows.Scan(&rel.TableName, &rel.ForeignTable, &rel.SQL)
		if err != nil {
			return
		}
		relations = append(relations, rel.toRelation())
	}
	relations = lo.Filter(relations, func(rel Relation, _ int) bool {
		return rel.ForeignTable == table
	})

	return
}
