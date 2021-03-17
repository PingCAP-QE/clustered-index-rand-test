package sqlgen

import (
	"math/rand"
	"reflect"
)

// ColumnTypeGroup stores column types -> Columns
type ColumnTypeGroup = map[ColumnType][]*Column

// GroupColumnsByColumnTypes groups all the Columns in given tables by ColumnType.
// This can be used in ON clause in JOIN.
func GroupColumnsByColumnTypes(tables ...*Table) ColumnTypeGroup {
	group := make(ColumnTypeGroup)
	for _, t := range tables {
		for _, c := range t.Columns {
			if _, ok := group[c.Tp]; ok {
				group[c.Tp] = append(group[c.Tp], c)
			} else {
				group[c.Tp] = []*Column{c}
			}
		}
	}
	return group
}

func RandColumnPairWithSameType(c ColumnTypeGroup) (*Column, *Column) {
	keys := reflect.ValueOf(c).MapKeys()
	randKey := keys[rand.Intn(len(keys))].Interface().(ColumnType)
	list := c[randKey]
	cnt := rand.Perm(len(list))
	cnt = cnt[:2]
	col1 := list[cnt[0]]
	col2 := list[cnt[1]]
	return col1, col2
}

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
		targetGroup := groups[rand.Intn(groupCount)]
		targetGroup = append(targetGroup, s)
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
