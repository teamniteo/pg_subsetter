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
	q := strings.Split(strings.TrimSpace(value), ":")

	*i = append(*i, subsetter.Rule{
		Table: strings.TrimSpace(q[0]),
		Where: strings.TrimSpace(q[1]),
	})
	return nil
}
