package subsetter

import (
	"os"
	"reflect"
	"testing"
)

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}

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
		{
			"Complex, related tables must be excluded",
			[]Table{{"simple", 10, []Relation{}}, {"complex", 10, []Relation{{"simple", "id", "complex", "simple_id"}}}},
			[]Table{{"simple", 10, []Relation{}}},
		},
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
	skipCI(t)
	src := getTestConnection()
	dst := getTestConnectionDst()
	initSchema(src)
	initSchema(dst)
	defer clearSchema(src)
	defer clearSchema(dst)

	s := &Sync{
		source:      src,
		destination: dst,
	}
	tables := []Table{{"simple", 10, []Relation{}}}

	if err := s.CopyTables(tables); err != nil {
		t.Errorf("Sync.CopyTables() error = %v", err)
	}

}
