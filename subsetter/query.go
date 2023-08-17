package subsetter

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
)

type Table struct {
	Name      string
	Rows      int
	Relations []Relation
}

func (t *Table) RelationNames() (names string) {
	rel := lo.Map(t.Relations, func(r Relation, _ int) string {
		return r.PrimaryTable + ">" + r.PrimaryColumn
	})
	if len(rel) > 0 {
		return strings.Join(rel, ", ")
	}
	return "none"
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

func GetKeys(q string, conn *pgxpool.Pool) (ids []string, err error) {
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		var id string

		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}

	}
	rows.Close()

	return
}

func GetPrimaryKeyName(table string, conn *pgxpool.Pool) (name string, err error) {
	q := fmt.Sprintf(`SELECT a.attname
	FROM   pg_index i
	JOIN   pg_attribute a ON a.attrelid = i.indrelid
	AND a.attnum = ANY(i.indkey)
	WHERE  i.indrelid = '%s'::regclass
	AND    i.indisprimary;`, table)
	rows, err := conn.Query(context.Background(), q)
	for rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
	}
	rows.Close()
	return
}

func DeleteRows(table string, where string, conn *pgxpool.Pool) (err error) {
	q := fmt.Sprintf(`DELETE FROM %s WHERE %s`, table, where)
	_, err = conn.Exec(context.Background(), q)
	return
}

func CopyQueryToString(query string, conn *pgxpool.Pool) (result string, err error) {
	q := fmt.Sprintf(`copy (%s) to stdout`, query)
	var buff bytes.Buffer
	c, err := conn.Acquire(context.Background())
	if err != nil {
		return
	}
	defer c.Release()
	if _, err = c.Conn().PgConn().CopyTo(context.Background(), &buff, q); err != nil {
		return
	}
	result = buff.String()

	return
}

func CopyTableToString(table string, limit int, where string, conn *pgxpool.Pool) (result string, err error) {
	q := fmt.Sprintf(`SELECT * FROM %s %s order by random() limit %d`, table, where, limit)
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
	defer c.Release()

	if _, err = c.Conn().PgConn().CopyFrom(context.Background(), &buff, q); err != nil {
		return
	}

	return
}

func CountRows(s string, conn *pgxpool.Pool) (count int, err error) {
	q := "SELECT count(*) FROM " + s
	err = conn.QueryRow(context.Background(), q).Scan(&count)
	if err != nil {
		return
	}
	return
}
