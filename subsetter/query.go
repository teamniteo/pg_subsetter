package subsetter

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type Table struct {
	Name       string
	Rows       int
	Relations  []Relation
	RequiredBy []Relation
}

// RelationNames returns a list of relation names in human readable format.
func (t *Table) RelationNames() (names []string) {
	names = lo.Map(t.Relations, func(r Relation, _ int) string {
		return r.PrimaryTable + ">" + r.PrimaryColumn
	})

	return
}

// IsSelfRelated returns true if a table is self related.
func (t *Table) IsSelfRelated() bool {
	for _, r := range t.Relations {
		if r.IsSelfRelated() {
			return true
		}
	}
	return false
}

// IsSelfRelated returns true if a table is self related.
func TableByName(tables []Table, name string) Table {
	return lo.FindOrElse(tables, Table{}, func(t Table) bool {
		return t.Name == name
	})
}

// GetTablesWithRows returns a list of tables with the number of rows in each table.
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
			// skip system tables that are marked public
			if strings.HasPrefix(table.Name, "pg_") {
				continue
			}

			// fix for tables with no rows
			if table.Rows == -1 {
				table.Rows = 0
			}

			// Do a precise count for small tables
			if table.Rows == 0 {
				table.Rows, err = CountRows(table.Name, conn)
				if err != nil {
					return nil, err
				}
			}

			// Get relations
			table.Relations = GetRelations(table.Name, conn)

			// Get reverse relations
			table.RequiredBy = GetRequiredBy(table.Name, conn)

			tables = append(tables, table)
		}

	}
	rows.Close()

	return
}

// GetKeys returns a list of keys from a query.
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

// GetPrimaryKeyName returns the name of the primary key for a table.
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

// DeleteRows deletes rows from a table.
func DeleteRows(table string, where string, conn *pgxpool.Pool) (err error) {
	q := fmt.Sprintf(`DELETE FROM %s WHERE %s`, table, where)
	_, err = conn.Exec(context.Background(), q)
	return
}

// CopyQueryToString copies a query to a string.
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

// CopyTableToString copies a table to a string.
func CopyTableToString(table string, limit string, where string, conn *pgxpool.Pool) (result string, err error) {
	maybeOrder := ""
	if lo.IsNotEmpty(where) {
		maybeOrder = "order by random()"
	}

	q := fmt.Sprintf(`SELECT * FROM %s %s %s %s`, table, where, maybeOrder, limit)
	log.Debug().Msgf("CopyTableToString query: %s", q)
	return CopyQueryToString(q, conn)
}

// CopyStringToTable copies a string to a table.
func CopyStringToTable(table string, data string, conn *pgxpool.Pool) (err error) {
	log.Debug().Msgf("CopyStringToTable query: %s", table)
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

// CountRows returns the number of rows in a table.
func CountRows(s string, conn *pgxpool.Pool) (count int, err error) {
	q := "SELECT count(*) FROM " + s
	err = conn.QueryRow(context.Background(), q).Scan(&count)
	if err != nil {
		return
	}
	return
}
