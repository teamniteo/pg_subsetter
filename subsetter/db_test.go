package subsetter

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

var testConnSrc *pgxpool.Pool
var onceTestSrc sync.Once
var testConnTrg *pgxpool.Pool
var onceTestTrg sync.Once

func getTestConnection() *pgxpool.Pool {
	DATABASE_URL := os.Getenv("DATABASE_URL")
	if DATABASE_URL == "" {
		DATABASE_URL = "postgres://test_source@localhost:5432/test_source?sslmode=disable"
	}

	onceTestSrc.Do(func() {
		c, err := pgxpool.New(context.Background(), DATABASE_URL)
		if err != nil {
			panic(err)
		}
		testConnSrc = c
	})

	return testConnSrc
}

func getTestConnectionDst() *pgxpool.Pool {
	DATABASE_URL := os.Getenv("DATABASE_URL")
	if DATABASE_URL == "" {
		DATABASE_URL = "postgres://test_target@localhost:5432/test_target?sslmode=disable"
	}

	onceTestTrg.Do(func() {
		c, err := pgxpool.New(context.Background(), DATABASE_URL)
		if err != nil {
			panic(err)
		}
		testConnTrg = c
	})

	return testConnTrg
}

func initSchema(conn *pgxpool.Pool) {

	_, err := conn.Exec(context.Background(), `
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

	if err != nil {
		panic(err)
	}
}

func populateTestsWithData(conn *pgxpool.Pool, table string, size int) {
	for i := 0; i < size; i++ {
		query := fmt.Sprintf("INSERT INTO %s (text) VALUES ('test%d') RETURNING id", table, i)
		var row string
		err := conn.QueryRow(context.Background(), query).Scan(&row)
		if err != nil {
			panic(err)
		}
		query = fmt.Sprintf("INSERT INTO relation (simple_id) VALUES ('%v')", row)

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			panic(err)
		}
	}
}

func clearSchema(conn *pgxpool.Pool) {
	_, err := conn.Exec(context.Background(), `
		ALTER TABLE relation DROP CONSTRAINT relation_simple_fk;
		DROP TABLE simple;
		DROP TABLE relation;
	`)
	if err != nil {
		panic(err)
	}
}
