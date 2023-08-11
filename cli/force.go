package main

import (
	"fmt"
	"strings"

	"niteo.co/subsetter/subsetter"
)

type arrayForce []subsetter.Force

func (i *arrayForce) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayForce) Set(value string) error {
	q := strings.SplitAfter(strings.TrimSpace(value), ":")

	*i = append(*i, subsetter.Force{
		Table: q[0],
		Where: q[1],
	})
	return nil
}
