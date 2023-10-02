package subsetter

import (
	"fmt"
	"math"
	"strconv"

	"github.com/samber/lo"
)

// GetTargetSet returns a subset of tables with the number of rows scaled by the fraction.
func GetTargetSet(fraction float64, tables []Table) []Table {
	return lo.Map(tables, func(table Table, i int) Table {
		table.Rows = int(math.Pow(10, math.Log10(float64(table.Rows))*fraction))
		return table
	})
}

func QuoteString(s string) string {
	// if string is a number, don't quote it
	if _, err := strconv.Atoi(s); err == nil {
		return s
	}
	return fmt.Sprintf(`'%s'`, s)
}
