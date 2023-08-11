package subsetter

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func getTestConnection() *pgx.Conn {
	DATABASE_URL := os.Getenv("DATABASE_URL")
	if DATABASE_URL == "" {
		DATABASE_URL = "postgres://test_source@localhost:5432/test_source?sslmode=disable"
	}

	conn, err := pgx.Connect(context.Background(), DATABASE_URL)
	if err != nil {
		panic(err)
	}
	return conn
}

func populateTests(conn *pgx.Conn) {
	conn.Exec(context.Background(), `
		CREATE TABLE simple (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			text TEXT
		);
		
		CREATE TABLE relation (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			simple_id UUID
		);

		ALTER TABLE relation ADD CONSTRAINT relation_simple_fk FOREIGN KEY (simple_id) REFERENCES simple(id);
	`)
}

func populateTestsWithData(conn *pgx.Conn, table string, size int) {
	for i := 0; i < size; i++ {
		query := fmt.Sprintf("INSERT INTO %s (text) VALUES ('test%d') RETURNING id", table, i)
		var row string
		conn.QueryRow(context.Background(), query).Scan(&row)
		query = fmt.Sprintf("INSERT INTO relation (simple_id) VALUES ('%v')", row)

		conn.Exec(context.Background(), query)

	}
}

func clearPopulateTests(conn *pgx.Conn) {
	conn.Exec(context.Background(), `
		ALTER TABLE relation DROP CONSTRAINT relation_simple_fk;
		DROP TABLE simple;
		DROP TABLE relation;
	`)
}
