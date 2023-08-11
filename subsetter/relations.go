package subsetter

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// GetRelations returns a list of tables that have a foreign key for particular table.
func GetRelations(table string, conn *pgx.Conn) (relations []string, err error) {

	q := `SELECT tc.table_name AS foreign_table_name
	FROM 
		information_schema.table_constraints AS tc 
		JOIN information_schema.key_column_usage AS kcu
		ON tc.constraint_name = kcu.constraint_name
		JOIN information_schema.constraint_column_usage AS ccu
		ON ccu.constraint_name = tc.constraint_name
	WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name = $1;`

	rows, err := conn.Query(context.Background(), q, table)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err == nil {
			relations = append(relations, table)
		}
	}
	rows.Close()
	return
}
