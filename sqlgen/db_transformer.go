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

type SeqGetter = func(idx int) interface{}
type SeqSetter = func(idx int, v interface{})

// AppendHead sets [1, 2, 3, 4] to [2, 3, 4, 1].
func AppendHead(beg, end int, get SeqGetter, set SeqSetter) {
	if beg == end {
		return
	}
	first := get(beg)
	for i := beg; i < end; i++ {
		set(i, get(i+1))
	}
	set(end, first)
}

// PrependTail sets [1, 2, 3, 4] to [4, 1, 2, 3].
func PrependTail(beg, end int, get SeqGetter, set SeqSetter) {
	if beg == end {
		return
	}
	next := get(beg)
	for i := beg + 1; i < end+1; i++ {
		old := get(i)
		set(i, next)
		next = old
	}
	set(beg, next)
}

func Move(srcIdx, destIdx int, get SeqGetter, set SeqSetter) {
	if srcIdx == destIdx {
		return
	} else if srcIdx < destIdx {
		AppendHead(srcIdx, destIdx, get, set)
	} else {
		PrependTail(destIdx, srcIdx, get, set)
	}
}

// Filter filters the columns inplace.
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

type TableColumns struct {
	Table   *Table
	Columns Columns
}

type TableColumnsArr []*TableColumns

func (ts Tables) TableColumnPairs() TableColumnsArr {
	arr := make(TableColumnsArr, 0, len(ts))
	for _, t := range ts {
		arr = append(arr, &TableColumns{
			Table:   t,
			Columns: t.Columns,
		})
	}
	return arr
}

func (tcs TableColumnsArr) Map(pred func(tc *TableColumns) *TableColumns) TableColumnsArr {
	return gMap(tcs, pred)
}

func (tcs TableColumnsArr) Filter(pred func(tc *TableColumns) bool) TableColumnsArr {
	return gFilter(tcs, pred)
}

func (tcs TableColumnsArr) Rand() *TableColumns {
	return gRand1(tcs)
}

func (tc *TableColumns) Rand() (*Table, *Column) {
	return tc.Table, tc.Columns.Rand()
}

func GetRandTableColumnWithTp(tbls Tables, tp ColumnType) (*Table, *Column) {
	return tbls.TableColumnPairs().Map(func(tc *TableColumns) *TableColumns {
		tc.Columns = tc.Columns.Filter(func(c *Column) bool {
			return c.Tp == tp
		})
		return tc
	}).Filter(func(tc *TableColumns) bool {
		return len(tc.Columns) > 0
	}).Rand().Rand()
}
