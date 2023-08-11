package subsetter

import (
	"context"
	"reflect"
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
		{"simple", 0.5, []Table{{"simple", 1000}}, []Table{{"simple", 31}}},
		{"simple", 0.5, []Table{{"simple", 10}}, []Table{{"simple", 3}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTargetSet(tt.fraction, tt.tables); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTargetSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
