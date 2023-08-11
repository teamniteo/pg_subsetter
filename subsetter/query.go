package subsetter

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Table struct {
	Name      string
	Rows      int
	Relations []string
}

func GetTables(conn *pgx.Conn) (tables []string, err error) {
	q := `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';`
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}
	rows.Close()
	return
}

func GetTablesWithRows(conn *pgx.Conn) (tables []Table, err error) {
	q := `SELECT relname, reltuples::int FROM pg_class,information_schema.tables WHERE table_schema = 'public' AND relname = table_name;`
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		var table Table

		if err := rows.Scan(&table.Name, &table.Rows); err == nil {
			// fix for tables with no rows
			if table.Rows == -1 {
				table.Rows = 0
			}
			tables = append(tables, table)
		}

	}
	rows.Close()

	return
}

func CopyTableToString(table string, limit int, conn *pgx.Conn) (result string, err error) {
	q := fmt.Sprintf(`copy (SELECT * FROM %s order by random() limit %d) to stdout`, table, limit)
	var buff bytes.Buffer
	if _, err = conn.PgConn().CopyFrom(context.Background(), &buff, q); err != nil {
		return
	}
	result = buff.String()
	return
}

func CopyStringToTable(table string, data string, conn *pgx.Conn) (err error) {
	q := fmt.Sprintf(`copy %s from stdin`, table)
	var buff bytes.Buffer
	buff.WriteString(data)
	if _, err = conn.PgConn().CopyFrom(context.Background(), &buff, q); err != nil {
		return
	}

	return
}
