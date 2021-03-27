package sqlgen

import (
	"math/rand"
)

// ColumnTypeGroup stores column types -> Columns
type ColumnTypeGroup = map[ColumnType][]*Column

func FilterUniqueColumns(c ColumnTypeGroup) ColumnTypeGroup {
	for k, list := range c {
		if len(list) <= 1 {
			delete(c, k)
		}
	}
	return c
}

// SwapOutParameterizedColumns substitute random Columns with `?` for prepare statements.
// It returns the substituted column in order. For example,
//   it may changes [a, b, c, d] to [a, ?, c, ?] and returns [b, d]
func SwapOutParameterizedColumns(cols []*Column) []*Column {
	if len(cols) == 0 {
		return nil
	}
	var result []*Column
	for {
		chosenIdx := rand.Intn(len(cols))
		if cols[chosenIdx].Name != "?" {
			result = append(result, cols[chosenIdx])
			cols[chosenIdx] = &Column{Name: "?"}
		}
		if RandomBool() {
			break
		}
	}
	return result
}

// Make a list of string into random groups, and filter the empty groups. For example,
//   [a, b, c, d, e] -> [[b, e], [c], [a, d]]
func RandomGroups(ss []string, groupCount int) [][]string {
	groups := make([][]string, groupCount)
	for _, s := range ss {
		idx := rand.Intn(groupCount)
		groups[idx] = append(groups[idx], s)
	}
	for i := 0; i < len(groups); i++ {
		if len(groups[i]) == 0 {
			groups[i], groups[len(groups)-1] = groups[len(groups)-1], groups[i]
			groups = groups[:len(groups)-1]
			i--
		}
	}
	return groups
}
