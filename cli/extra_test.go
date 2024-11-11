package main

import (
	"fmt"
	"testing"
)

func Test_arrayExtra_Set(t *testing.T) {

	tests := []struct {
		name    string
		value   string
		rules   arrayExtra
		wantErr bool
	}{
		{"With tables", "simple: id < 10", arrayExtra{{Table: "simple", Where: "id < 10"}}, false},
		{"With tables and all", "simple: all", arrayExtra{{Table: "simple", Where: "1=1"}}, false},
		{"With tables only", "simple", arrayExtra{{Table: "simple"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := arrayExtra{}
			if err := r.Set(tt.value); (err != nil) != tt.wantErr {
				t.Errorf("arrayExtra.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if fmt.Sprintf("%v", r) != fmt.Sprintf("%v", tt.rules) {
				t.Errorf("arrayExtra.Set() = %v, want %v", r, tt.rules)
			}
		})
	}
}
