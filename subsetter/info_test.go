package subsetter

import (
	"testing"
)

func TestGetTargetSet(t *testing.T) {
	conn := getTestConnection()
	initSchema(conn)
	defer clearSchema(conn)

	tests := []struct {
		name     string
		fraction float64
		tables   []Table
		want     []Table
	}{
		{"simple", 0.5, []Table{{"simple", 1000, []Relation{}}}, []Table{{"simple", 31, []Relation{}}}},
		{"simple", 0.5, []Table{{"simple", 10, []Relation{}}}, []Table{{"simple", 3, []Relation{}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTargetSet(tt.fraction, tt.tables); got[0].Rows != tt.want[0].Rows {
				t.Errorf("GetTargetSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
