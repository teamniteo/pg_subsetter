package subsetter

import (
	"slices"

	"github.com/stevenle/topsort"
)

func TableGraph(primary string, relations []Relation) (l []string, e error) {
	graph := topsort.NewGraph() // Create a new graph

	for _, r := range relations {
		if !r.IsSelfRelated() {
			graph.AddEdge(r.PrimaryTable, r.ForeignTable)
		}
	}
	l, e = graph.TopSort(primary)
	slices.Reverse(l)
	return
}
