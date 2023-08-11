package subsetter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Relation struct {
	PrimaryTable  string
	PrimaryColumn string
	ForeignTable  string
	ForeignColumn string
}

func (r *Relation) Query() string {
	return fmt.Sprintf(`SELECT * FROM %s WHERE %s IN (SELECT %s FROM %s)`, r.ForeignTable, r.ForeignColumn, r.PrimaryColumn, r.PrimaryTable)
}

// GetRelations returns a list of tables that have a foreign key for particular table.
func GetRelations(table string, conn *pgxpool.Pool) (relations []Relation, err error) {

	q := `SELECT
		kcu.table_name AS foreign_table_name,
		kcu.column_name AS foreign_column_name,
		ccu.table_name,
		ccu.column_name
	FROM
		information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name
			AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name
	WHERE
		tc.constraint_type = 'FOREIGN KEY'
		AND ccu.table_name = $1
		AND tc.table_schema = 'public';`

	rows, err := conn.Query(context.Background(), q, table)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var rel Relation
		err = rows.Scan(&rel.ForeignTable, &rel.ForeignColumn, &rel.PrimaryTable, &rel.PrimaryColumn)
		if err != nil {
			return
		}
		relations = append(relations, rel)
	}

	return
}
