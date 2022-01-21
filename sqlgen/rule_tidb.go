package sqlgen

import (
	"fmt"
	"math/rand"
)

var AdminCheck = NewFn(func(state *State) Fn {
	state.env.Table = state.GetRandTable()
	return Or(
		AdminCheckTable,
		AdminCheckIndex.P(CurrentTableHasIndices),
	)
}).P(HasTables)

var AdminCheckTable = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	return Strs("admin check table", tbl.Name)
})

var AdminCheckIndex = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	idx := tbl.GetRandomIndex()
	return Strs("admin check index", tbl.Name, idx.Name)
})

var FlashBackTable = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	state.InjectTodoSQL(fmt.Sprintf("/*DDL*/ flashback table %s", tbl.Name))
	return Or(
		Strs("drop table", tbl.Name),
		Strs("truncate table", tbl.Name),
	)
})

var SetTiFlashReplica = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	tbl.tiflashReplica = 1
	return Strs("alter table", tbl.Name, "set tiflash replica 1")
})

var SplitRegion = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	splitTablePrefix := fmt.Sprintf("split table %s", tbl.Name)

	splittingIndex := len(tbl.Indices) > 0 && RandomBool()
	var idx *Index
	var idxPrefix string
	if splittingIndex {
		idx = tbl.Indices[rand.Intn(len(tbl.Indices))]
		idxPrefix = fmt.Sprintf("index %s", idx.Name)
	}

	// split table t between (1, 2) and (100, 200) regions 2;
	var splitTableRegionBetween = NewFn(func(state *State) Fn {
		rows := tbl.GenMultipleRowsAscForHandleCols(2)
		low, high := rows[0], rows[1]
		return Strs(splitTablePrefix, "between",
			"(", PrintRandValues(low), ")", "and",
			"(", PrintRandValues(high), ")", "regions", RandomNum(2, 10))
	})

	// split table t index idx between (1, 2) and (100, 200) regions 2;
	var splitIndexRegionBetween = NewFn(func(state *State) Fn {
		rows := tbl.GenMultipleRowsAscForIndexCols(2, idx)
		low, high := rows[0], rows[1]
		return Strs(splitTablePrefix, idxPrefix, "between",
			"(", PrintRandValues(low), ")", "and",
			"(", PrintRandValues(high), ")", "regions", RandomNum(2, 10))
	})

	// split table t by ((1, 2), (100, 200));
	var splitTableRegionBy = NewFn(func(state *State) Fn {
		rows := tbl.GenMultipleRowsAscForHandleCols(rand.Intn(10) + 2)
		return Strs(splitTablePrefix, "by", PrintSplitByItems(rows))
	})

	// split table t index idx by ((1, 2), (100, 200));
	var splitIndexRegionBy = NewFn(func(state *State) Fn {
		rows := tbl.GenMultipleRowsAscForIndexCols(rand.Intn(10)+2, idx)
		return Strs(splitTablePrefix, idxPrefix, "by", PrintSplitByItems(rows))
	})

	if splittingIndex {
		return Or(splitIndexRegionBetween, splitIndexRegionBy)
	}
	return Or(splitTableRegionBetween, splitTableRegionBy)
})
