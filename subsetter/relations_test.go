package subsetter

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestGetRelations(t *testing.T) {
	conn := getTestConnection()
	populateTests(conn)
	defer conn.Close(context.Background())
	defer clearPopulateTests(conn)
	tests := []struct {
		name          string
		table         string
		conn          *pgx.Conn
		wantRelations []Relation
	}{
		{"With relation", "simple", conn, []Relation{{"relation", "simple_id"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRelations, _ := GetRelations(tt.table, tt.conn); !reflect.DeepEqual(gotRelations, tt.wantRelations) {
				t.Errorf("GetRelations() = %v, want %v", gotRelations, tt.wantRelations)
			}
		})
	}
}
