package subsetter

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
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
		{"With relation", "relation", conn, []Relation{{"relation", "simple_id", "simple", "id"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRelations := GetRelations(tt.table, tt.conn); !reflect.DeepEqual(gotRelations, tt.wantRelations) {
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
		{"Simple", Relation{"simple", "id", "relation", "simple_id"}, "SELECT * FROM simple WHERE id IN (1)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.Query([]string{"1"}); got != tt.want {
				t.Errorf("Relation.Query() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRelationRaw_toRelation(t *testing.T) {

	tests := []struct {
		name   string
		fields RelationRaw
		want   Relation
	}{
		{
			"Simple",
			RelationRaw{"relation", "simple", "FOREIGN KEY (simple_id) REFERENCES simple(id)"},
			Relation{"relation", "simple_id", "simple", "id"},
		},
		{
			"Simple with cascade",
			RelationRaw{"relation", "simple", "FOREIGN KEY (simple_id) REFERENCES simple(id) ON DELETE CASCADE"},
			Relation{"relation", "simple_id", "simple", "id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RelationRaw{
				PrimaryTable: tt.fields.PrimaryTable,
				ForeignTable: tt.fields.ForeignTable,
				SQL:          tt.fields.SQL,
			}
			if got := r.toRelation(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RelationRaw.toRelation() = %v, want %v", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}
