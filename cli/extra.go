package main

import (
	"fmt"
	"strings"

	"niteo.co/subsetter/subsetter"
)

type arrayExtra []subsetter.Rule

func (i *arrayExtra) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayExtra) Set(value string) error {
	q := strings.SplitN(strings.TrimSpace(value), ":", 2)

	*i = append(*i, subsetter.Rule{
		Table: strings.TrimSpace(q[0]),
		Where: maybeAll(strings.TrimSpace(q[1])),
	})
	return nil
}

func maybeAll(s string) string {
	if s == "all" {
		return subsetter.RuleAll
	}
	return s
}
