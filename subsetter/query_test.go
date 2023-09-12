package subsetter

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGetTablesWithRows(t *testing.T) {
	conn := getTestConnection()
	initSchema(conn)
	defer clearSchema(conn)
	tests := []struct {
		name       string
		conn       *pgxpool.Pool
		wantTables []Table
		wantErr    bool
	}{
		{"With tables", conn, []Table{{"simple", 0, []Relation{}}, {"relation", 0, []Relation{}}}, false},
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
	initSchema(conn)
	defer clearSchema(conn)
	populateTestsWithData(conn, "simple", 10)

	tests := []struct {
		name       string
		table      string
		conn       *pgxpool.Pool
		wantResult bool
		wantErr    bool
	}{
		{"With tables", "simple", conn, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, err := CopyTableToString(tt.table, "", "", tt.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("CopyTableToString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if strings.Contains(gotResult, "test") != tt.wantResult {
				t.Errorf("CopyTableToString() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func TestCopyStringToTable(t *testing.T) {
	conn := getTestConnection()
	initSchema(conn)
	defer clearSchema(conn)

	tests := []struct {
		name       string
		table      string
		data       string
		conn       *pgxpool.Pool
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
			gotInserted, _ := CountRows(tt.table, tt.conn)
			if tt.wantResult != gotInserted {
				t.Errorf("CopyStringToTable() = %v, want %v", tt.wantResult, tt.wantResult)
			}

		})
	}
}

func TestDeleteRows(t *testing.T) {

	conn := getTestConnection()
	initSchema(conn)
	defer clearSchema(conn)
	tests := []struct {
		name    string
		conn    *pgxpool.Pool
		table   string
		where   string
		count   int
		wantErr bool
	}{
		{"With tables", conn, "simple", "1 = 1", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := DeleteRows(tt.table, tt.where, tt.conn); (err != nil) != tt.wantErr {
				t.Errorf("DeleteRows() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotCount, _ := CountRows(tt.table, tt.conn); gotCount != tt.count {
				t.Errorf("DeleteRows() = %v, want %v", gotCount, tt.count)
			}
		})
	}
}
