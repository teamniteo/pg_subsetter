package subsetter

import (
	"fmt"
	"math"
	"strconv"
)

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

func QuoteString(s string) string {
	// if string is a number, don't quote it
	if _, err := strconv.Atoi(s); err == nil {
		return s
	}
	return fmt.Sprintf(`'%s'`, s)
}
