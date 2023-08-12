package subsetter

import (
	"reflect"
	"testing"
)

func TestViableSubset(t *testing.T) {
	tests := []struct {
		name       string
		tables     []Table
		wantSubset []Table
	}{
		{
			"Simple",
			[]Table{{"simple", 10, []Relation{}}},
			[]Table{{"simple", 10, []Relation{}}},
		},
		{
			"No rows",
			[]Table{{"simple", 0, []Relation{}}},
			[]Table{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotSubset := ViableSubset(tt.tables); !reflect.DeepEqual(gotSubset, tt.wantSubset) {
				t.Errorf("ViableSubset() = %v, want %v", gotSubset, tt.wantSubset)
			}
		})
	}
}

func TestSync_CopyTables(t *testing.T) {
	src := getTestConnection()
	dst := getTestConnectionDst()
	initSchema(src)
	initSchema(dst)
	defer clearSchema(src)
	defer clearSchema(dst)

	populateTestsWithData(src, "simple", 1000)

	s := &Sync{
		source:      src,
		destination: dst,
	}
	tables := []Table{{"simple", 10, []Relation{}}}

	if err := s.CopyTables(tables); err != nil {
		t.Errorf("Sync.CopyTables() error = %v", err)
	}

}
