package subsetter

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestGetRelations(t *testing.T) {
	conn := getTestConnection()
	initSchema(conn)
	defer clearSchema(conn)
	tests := []struct {
		name          string
		table         string
		conn          *pgxpool.Pool
		wantRelations []Relation
	}{
		{"With relation", "simple", conn, []Relation{{"simple", "id", "relation", "simple_id"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRelations, _ := GetRelations(tt.table, tt.conn); !reflect.DeepEqual(gotRelations, tt.wantRelations) {
				t.Errorf("GetRelations() = %v, want %v", gotRelations, tt.wantRelations)
			}
		})
	}
}

func TestRelation_Query(t *testing.T) {
	tests := []struct {
		name string
		r    Relation
		want string
	}{
		{"Simple", Relation{"simple", "id", "relation", "simple_id"}, "SELECT * FROM relation WHERE simple_id IN (SELECT id FROM simple)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.Query(); got != tt.want {
				t.Errorf("Relation.Query() = %v, want %v", got, tt.want)
			}
		})
	}
}
