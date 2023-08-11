package subsetter

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// GetRelations returns a list of tables that have a foreign key for particular table.
func GetRelations(table string, conn *pgx.Conn) (relations []string, err error) {

	q := `SELECT ccu.table_name::string AS foreign_table_name
	FROM 
		information_schema.table_constraints AS tc 
		JOIN information_schema.key_column_usage AS kcu
		ON tc.constraint_name = kcu.constraint_name
		JOIN information_schema.constraint_column_usage AS ccu
		ON ccu.constraint_name = tc.constraint_name
	WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;`

	err = conn.QueryRow(context.Background(), q, table).Scan(&relations)
	return
}
