package subsetter

import "math"

// GetTargetSet returns a subset of tables with the number of rows scaled by the fraction.
func GetTargetSet(fraction float64, tables []Table) []Table {
	var subset []Table

	for _, table := range tables {
		subset = append(subset, Table{
			Name:      table.Name,
			Rows:      int(math.Pow(10, math.Log10(float64(table.Rows))*fraction)),
			Relations: table.Relations,
		})
	}

	return subset
}
