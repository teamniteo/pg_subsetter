package subsetter

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type Table struct {
	Name string
	Rows int
}

func GetTables(conn *pgx.Conn) (tables []string, err error) {
	q := `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';`
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		var name string
		rows.Scan(&name)
		tables = append(tables, name)
	}
	rows.Close()
	return
}

func GetTablesWithRows(conn *pgx.Conn) (tables []Table, err error) {
	q := `SELECT relname, reltuples::int FROM pg_class,information_schema.tables WHERE table_schema = 'public' AND relname = table_name;`
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		var table Table
		rows.Scan(&table.Name, &table.Rows)

		// fix for tables with no rows
		if table.Rows == -1 {
			table.Rows = 0
		}
		tables = append(tables, table)
	}
	rows.Close()

	return
}
