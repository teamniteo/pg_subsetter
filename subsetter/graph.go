package subsetter

import (
	"slices"

	"github.com/stevenle/topsort"
)

// TableGraph generates a topologically sorted list of table names based on their relations.
// It takes a primary table name and a slice of Relation objects as input.
// The function returns a slice of strings representing the sorted table names and an error if any.
//
// Parameters:
//   - primary: The name of the primary table to start the topological sort from.
//   - relations: A slice of Relation objects representing the relationships between tables.
//
// Returns:
//   - l: A slice of strings representing the topologically sorted table names.
//   - err: An error if the topological sort fails or if there is an issue adding edges to the graph.
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

// RequiredTableGraph generates a list of required tables in topological order
// starting from the primary table. It uses the provided relations to build a
// directed graph and performs a topological sort.
//
// Parameters:
// - primary: The name of the primary table to start the topological sort from.
// - relations: A slice of Relation structs that define the relationships between tables.
//
// Returns:
// - l: A slice of strings representing the tables in topological order.
// - err: An error if the graph construction or topological sort fails.
func RequiredTableGraph(primary string, relations []Relation) (l []string, err error) {
	graph := topsort.NewGraph() // Create a new graph

	for _, r := range relations {
		if !r.IsSelfRelated() {
			err = graph.AddEdge(r.ForeignTable, r.PrimaryTable)
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
