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

func ConcatColumns(col1, col2 []*Column) []*Column {
	ret := make([]*Column, 0, len(col1)+len(col2))
	ret = append(ret, col1...)
	ret = append(ret, col2...)
	return ret
}

// FilterColumns filters the columns inplace.
func FilterColumnGroup(colGroups [][]*Column, pred func(c []*Column) bool) [][]*Column {
	filled := 0
	for _, g := range colGroups {
		if pred(g) {
			colGroups[filled] = g
			filled++
		}
	}
	return colGroups[:filled]
}

// RandomCompatibleColumnPair select 2 columns that have the compatible types.
// This method assumes cols1 and cols2 are non-empty.
func RandomCompatibleColumnPair(cols1, cols2 []*Column) (col1 *Column, col2 *Column) {
	Assert(len(cols1) > 0 && len(cols2) > 0)
	groupAndShuffle := func(cs []*Column) [][]*Column {
		g := GroupColumnsWithSameType(cs)
		rand.Shuffle(len(g), func(i, j int) {
			g[i], g[j] = g[j], g[i]
		})
		return g
	}
	colGroup1 := groupAndShuffle(cols1)
	colGroup2 := groupAndShuffle(cols2)
	for _, g1 := range colGroup1 {
		for _, g2 := range colGroup2 {
			if g1[0].Tp.SameTypeAs(g2[0].Tp) {
				return g1[rand.Intn(len(g1))], g2[rand.Intn(len(g2))]
			}
		}
	}
	return cols1[rand.Intn(len(cols1))], cols2[rand.Intn(len(cols2))]
}

// Group the columns with the same type.
func GroupColumnsWithSameType(cols []*Column) [][]*Column {
	var result [][]*Column
	for _, c := range cols {
		found := false
		for i, r := range result {
			if len(r) != 0 && r[0].Tp.SameTypeAs(c.Tp) {
				found = true
				result[i] = append(result[i], c)
				break
			}
		}
		if !found {
			result = append(result, []*Column{c})
		}
	}
	return result
}
