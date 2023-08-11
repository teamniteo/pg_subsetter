package subsetter

import (
	"context"
	"reflect"
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
		{"With tables", conn, []Table{{"simple", 0}, {"relation", 0}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTables, err := GetTablesWithRows(tt.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTablesWithRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTables, tt.wantTables) {
				t.Errorf("GetTablesWithRows() = %v, want %v", gotTables, tt.wantTables)
			}
		})
	}
}
