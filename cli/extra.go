package main

import (
	"fmt"
	"strings"

	"niteo.co/subsetter/subsetter"
)

type arrayExtra []subsetter.Rule

func (ae *arrayExtra) String() string {
	return fmt.Sprintf("%v", *ae)
}

func (ae *arrayExtra) Set(value string) error {
	q := strings.SplitN(strings.TrimSpace(value), ":", 2)

	table := strings.TrimSpace(q[0])
	where := ""
	if len(q) > 1 {
		where = strings.TrimSpace(q[1])
	}

	*ae = append(*ae, subsetter.Rule{
		Table: table,
		Where: maybeAll(where),
	})
	return nil
}

func maybeAll(s string) string {
	if s == "all" {
		return subsetter.RuleAll
	}
	return s
}
