package subsetter

import (
	"context"
	"testing"
)

func TestGetTargetSet(t *testing.T) {
	conn := getTestConnection()
	populateTests(conn)
	defer conn.Close(context.Background())
	defer clearPopulateTests(conn)

	tests := []struct {
		name     string
		fraction float64
		tables   []Table
		want     []Table
	}{
		{"simple", 0.5, []Table{{"simple", 1000, []string{}}}, []Table{{"simple", 31, []string{}}}},
		{"simple", 0.5, []Table{{"simple", 10, []string{}}}, []Table{{"simple", 3, []string{}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTargetSet(tt.fraction, tt.tables); got[0].Rows != tt.want[0].Rows {
				t.Errorf("GetTargetSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
