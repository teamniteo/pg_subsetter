package subsetter

import (
	"slices"

	"github.com/stevenle/topsort"
)

func TableGraph(primary string, relations []Relation) (l []string, err error) {
	graph := topsort.NewGraph() // Create a new graph

	for _, r := range relations {
		if !r.IsSelfRelated() {
			err = graph.AddEdge(r.PrimaryTable, r.ForeignTable)
			if err != nil {
				return
			}
		}
	}
	l, err = graph.TopSort(primary)
	if err != nil {
		return
	}
	slices.Reverse(l)
	return
}
