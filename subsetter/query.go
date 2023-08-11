package subsetter

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Table struct {
	Name      string
	Rows      int
	Relations []Relation
}

func GetTablesWithRows(conn *pgxpool.Pool) (tables []Table, err error) {
	q := `SELECT
		relname,
		reltuples::int
	FROM
		pg_class,
		information_schema.tables
	WHERE
		table_schema = 'public'
		AND relname = table_name;`
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		var table Table

		if err := rows.Scan(&table.Name, &table.Rows); err == nil {
			// fix for tables with no rows
			if table.Rows == -1 {
				table.Rows = 0
			}
			table.Relations, err = GetRelations(table.Name, conn)
			if err != nil {
				return nil, err
			}
			tables = append(tables, table)
		}

	}
	rows.Close()

	return
}

func CopyQueryToString(query string, conn *pgxpool.Pool) (result string, err error) {
	q := fmt.Sprintf(`copy (%s) to stdout`, query)
	var buff bytes.Buffer
	c, err := conn.Acquire(context.Background())
	if err != nil {
		return
	}
	if _, err = c.Conn().PgConn().CopyTo(context.Background(), &buff, q); err != nil {
		return
	}
	result = buff.String()
	return
}

func CopyTableToString(table string, limit int, conn *pgxpool.Pool) (result string, err error) {
	q := fmt.Sprintf(`SELECT * FROM %s order by random() limit %d`, table, limit)
	return CopyQueryToString(q, conn)
}

func CopyStringToTable(table string, data string, conn *pgxpool.Pool) (err error) {
	q := fmt.Sprintf(`copy %s from stdin`, table)
	var buff bytes.Buffer
	buff.WriteString(data)
	c, err := conn.Acquire(context.Background())
	if err != nil {
		return
	}
	if _, err = c.Conn().PgConn().CopyFrom(context.Background(), &buff, q); err != nil {
		return
	}

	return
}
