package subsetter

import (
	"testing"
)

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
