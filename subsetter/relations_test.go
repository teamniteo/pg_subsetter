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
		{"With relation", "simple", conn, []Relation{{"relation", "simple_id", "simple", "id"}}},
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

func TestRelationInfo_toRelation(t *testing.T) {

	tests := []struct {
		name   string
		fields RelationInfo
		want   Relation
	}{
		{
			"Simple",
			RelationInfo{"relation", "simple", "FOREIGN KEY (simple_id) REFERENCES simple(id)"},
			Relation{"relation", "simple_id", "simple", "id"},
		},
		{
			"Simple with cascade",
			RelationInfo{"relation", "simple", "FOREIGN KEY (simple_id) REFERENCES simple(id) ON DELETE CASCADE"},
			Relation{"relation", "simple_id", "simple", "id"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RelationInfo{
				TableName:    tt.fields.TableName,
				ForeignTable: tt.fields.ForeignTable,
				SQL:          tt.fields.SQL,
			}
			if got := r.toRelation(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RelationInfo.toRelation() = %v, want %v", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}
