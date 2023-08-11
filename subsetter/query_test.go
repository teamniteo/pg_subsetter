package subsetter

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestGetTables(t *testing.T) {
	conn := getTestConnection()
	populateTests(conn)
	defer conn.Close(context.Background())
	defer clearPopulateTests(conn)
	tests := []struct {
		name       string
		conn       *pgx.Conn
		wantTables []string
	}{
		{"With tables", conn, []string{"simple", "relation"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotTables, _ := GetTables(tt.conn); !reflect.DeepEqual(gotTables, tt.wantTables) {
				t.Errorf("GetTables() = %v, want %v", gotTables, tt.wantTables)
			}
		})
	}
}

func TestGetTablesWithRows(t *testing.T) {
	conn := getTestConnection()
	populateTests(conn)
	defer conn.Close(context.Background())
	defer clearPopulateTests(conn)
	tests := []struct {
		name       string
		conn       *pgx.Conn
		wantTables []Table
		wantErr    bool
	}{
		{"With tables", conn, []Table{{"simple", 0, []string{}}, {"relation", 0, []string{}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTables, err := GetTablesWithRows(tt.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTablesWithRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotTables[0].Name != tt.wantTables[0].Name {
				t.Errorf("GetTablesWithRows() = %v, want %v", gotTables, tt.wantTables)
			}
			if gotTables[0].Rows != tt.wantTables[0].Rows {
				t.Errorf("GetTablesWithRows() = %v, want %v", gotTables, tt.wantTables)
			}
		})
	}
}

func TestCopyTableToString(t *testing.T) {
	conn := getTestConnection()
	populateTests(conn)
	defer conn.Close(context.Background())
	defer clearPopulateTests(conn)
	populateTestsWithData(conn, "simple", 10)

	tests := []struct {
		name       string
		table      string
		conn       *pgx.Conn
		wantResult bool
		wantErr    bool
	}{
		{"With tables", "simple", conn, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, err := CopyTableToString(tt.table, 10, tt.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("CopyTableToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if strings.Contains(gotResult, "test") == tt.wantResult {
				t.Errorf("CopyTableToString() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func TestCopyStringToTable(t *testing.T) {
	conn := getTestConnection()
	populateTests(conn)
	defer conn.Close(context.Background())
	defer clearPopulateTests(conn)

	tests := []struct {
		name       string
		table      string
		data       string
		conn       *pgx.Conn
		wantResult int
		wantErr    bool
	}{
		{"With tables", "simple", "cccc5f58-44d3-4d7a-bf37-a97d4f081a63	test\n", conn, 1, false},
		{"With more tables", "simple", "edcd63fe-303e-4d51-83ea-3fd00740ba2c	test4\na170b0f5-3aec-469c-9589-cf25888a72e2	test7", conn, 3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CopyStringToTable(tt.table, tt.data, tt.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("CopyStringToTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			gotInserted := insertedRows(tt.table, tt.conn)
			if tt.wantResult != gotInserted {
				t.Errorf("CopyStringToTable() = %v, want %v", tt.wantResult, tt.wantResult)
			}

		})
	}
}

func insertedRows(s string, conn *pgx.Conn) int {
	q := "SELECT count(*) FROM " + s
	var count int
	err := conn.QueryRow(context.Background(), q).Scan(&count)
	if err != nil {
		panic(err)
	}
	return count
}
