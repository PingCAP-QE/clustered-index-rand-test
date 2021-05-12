package sqlgen

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"path"
	"strings"
	"time"

	"github.com/zyguan/sqlz/resultset"
)

func NewGenerator(state *State) func() string {
	rand.Seed(time.Now().UnixNano())
	return func() string {
		return start.Eval(state)
	}
}

var start = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	if s, ok := state.PopOneTodoSQL(); ok {
		return Str(s)
	}
	if !state.Initialized() {
		eInitStart := Str(initStart.Eval(state))
		if state.MeetInitializedDemand() {
			state.SetInitialized()
		}
		return eInitStart
	}
	return Or(
		switchRowFormatVer.SetW(w.SetRowFormat),
		switchClustered.SetW(w.SetClustered),
		adminCheck.SetW(w.AdminCheck),
		createTable.SetW(w.CreateTable),
		createTableLike.SetW(w.CreateTable),
		dmlStmt.SetW(w.Query_DML),
		ddlStmt.SetW(w.Query_DDL),
		splitRegion.SetW(w.Query_Split),
		commonAnalyze.SetW(w.Query_Analyze),
		prepareStmt.SetW(w.Query_Prepare),
		deallocPrepareStmt.SetW(1),
		flashBackTable.SetW(1),
		selectIntoOutFile.SetW(1),
		loadTable.SetW(1),
		dropTable.SetW(1),
	)
})

var initStart = NewFn(func(state *State) Fn {
	Assert(!state.Initialized())
	if len(state.tables) < state.ctrl.InitTableCount {
		return createTable
	} else {
		return insertInto
	}
})

var dmlStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	w := state.ctrl.Weight
	return Or(
		query.SetW(w.Query_Select),
		queryPrepare,
		commonDelete.SetW(w.Query_DML_DEL),
		commonInsert.SetW(w.Query_DML_INSERT),
		commonUpdate.SetW(w.Query_DML_UPDATE),
	)
})

var ddlStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
	return Or(
		addColumn,
		addIndex,
		dropColumn,
		dropIndex,
		alterColumn,
	)
})

var switchRowFormatVer = NewFn(func(state *State) Fn {
	if RandomBool() {
		return Str("set @@global.tidb_row_format_version = 2")
	}
	return Str("set @@global.tidb_row_format_version = 1")
})

var switchClustered = NewFn(func(state *State) Fn {
	if RandomBool() {
		return Str("set @@global.tidb_enable_clustered_index = 0")
	}
	return Str("set @@global.tidb_enable_clustered_index = 1")
})

var dropTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	return Strs("drop table", tbl.Name)
})

var flashBackTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, CanReadGCSavePoint) {
		return None()
	}
	tbl := state.GetRandTable()
	state.InjectTodoSQL(fmt.Sprintf("flashback table %s", tbl.Name))
	return Or(
		Strs("drop table", tbl.Name),
		Strs("truncate table", tbl.Name),
	)
})

var adminCheck = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	if state.ctrl.EnableTestTiFlash {
		// Error: Error 1815: Internal : Can't find a proper physical plan for this query
		// https://github.com/pingcap/tidb/issues/22947
		return Str("")
	} else {
		if len(tbl.Indices) == 0 {
			return Strs("admin check table", tbl.Name)
		}
		idx := tbl.GetRandomIndex()
		return Or(
			Strs("admin check table", tbl.Name),
			Strs("admin check index", tbl.Name, idx.Name),
		)
	}
})

var createTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(NoTooMuchTables) {
		return None()
	}
	tbl := GenNewTable(state.AllocGlobalID(ScopeKeyTableUniqID))
	state.AppendTable(tbl)
	state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
	if state.ctrl.EnableTestTiFlash {
		state.InjectTodoSQL(fmt.Sprintf("alter table %s set tiflash replica 1", tbl.Name))
		state.InjectTodoSQL(fmt.Sprintf("select sleep(20)"))
	}
	// The eval order matters because the dependency is colDefs <- partitionDef <- idxDefs.
	eColDefs := colDefs.Eval(state)
	state.Store(ScopeKeyCurrentPartitionColumn, NewScopeObj(tbl.GetRandColumnForPartition()))
	ePartitionDef := partitionDef.Eval(state)
	eIdxDefs := idxDefs.Eval(state)
	return Strs("create table", tbl.Name, "(", eColDefs, eIdxDefs, ")", ePartitionDef)
})

var colDefs = NewFn(func(state *State) Fn {
	Assert(!state.Search(ScopeKeyCurrentTable).IsNil())
	if !state.Initialized() {
		return Repeat(colDef, state.ctrl.InitColCount, Str(","))
	}
	return RepeatRange(1, 10, colDef, Str(","))
})

var colDef = NewFn(func(state *State) Fn {
	Assert(!state.Search(ScopeKeyCurrentTable).IsNil())
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	col := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	tbl.AppendColumn(col)
	return And(Str(col.Name), Str(PrintColumnType(col)))
})

var idxDefs = NewFn(func(state *State) Fn {
	Assert(state.Exists(ScopeKeyCurrentTable))
	if RandomBool() {
		return Empty()
	}
	return And(
		Str(","),
		RepeatRange(1, 4, idxDef, Str(",")),
	)
})

var idxDef = NewFn(func(state *State) Fn {
	Assert(!state.Search(ScopeKeyCurrentTable).IsNil())
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	idx := GenNewIndex(state, state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
	if idx.IsUnique() && state.Exists(ScopeKeyCurrentPartitionColumn) {
		partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
		// all partitioned Columns should be contained in every unique/primary index.
		idx.AppendColumnIfNotExists(partitionedCol)
	}
	tbl.AppendIndex(idx)
	return And(
		Str(PrintIndexType(idx)), Str("key"), Str(idx.Name),
		Str("("), Str(PrintIndexColumnNames(idx)), Str(")"),
		OptIf(idx.Tp == IndexTypePrimary && state.ExistsConfig(ConfigKeyUnitPKNeedClusteredHint),
			Str("/*T![clustered_index] clustered */"),
		),
	)
})

var partitionDef = NewFn(func(state *State) Fn {
	Assert(state.Exists(ScopeKeyCurrentTable))
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	if !state.Exists(ScopeKeyCurrentPartitionColumn) {
		return Empty()
	}
	partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
	tbl.AppendPartitionColumn(partitionedCol)
	return Or(
		Empty(),
		partitionDefHash,
		partitionDefRange,
		partitionDefList,
	)
})

var partitionDefHash = NewFn(func(state *State) Fn {
	Assert(!state.Search(ScopeKeyCurrentPartitionColumn).IsNil())
	partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
	partitionNum := RandomNum(1, 6)
	return And(
		Str("partition by hash ("),
		Str(partitionedCol.Name),
		Str(")"),
		Str("partitions"),
		Str(partitionNum),
	)
})

var partitionDefRange = NewFn(func(state *State) Fn {
	Assert(!state.Search(ScopeKeyCurrentPartitionColumn).IsNil())
	partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
	partitionCount := rand.Intn(5) + 1
	vals := partitionedCol.RandomValuesAsc(partitionCount)
	if rand.Intn(2) == 0 {
		partitionCount++
		vals = append(vals, "maxvalue")
	}
	return Strs(
		"partition by range (",
		partitionedCol.Name, ") (",
		PrintRangePartitionDefs(vals),
		")",
	)
})

var partitionDefList = NewFn(func(state *State) Fn {
	Assert(!state.Search(ScopeKeyCurrentPartitionColumn).IsNil())
	partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
	listVals := partitionedCol.RandomValuesAsc(20)
	listGroups := RandomGroups(listVals, rand.Intn(3)+1)
	return Strs(
		"partition by list (",
		partitionedCol.Name, ") (",
		PrintListPartitionDefs(listGroups),
		")",
	)
})

var insertInto = NewFn(func(state *State) Fn {
	Assert(!state.Initialized())
	tbl := state.GetFirstNonFullTable()
	vals := tbl.GenRandValues(tbl.Columns)
	tbl.AppendRow(vals)
	return And(
		Str("insert into"),
		Str(tbl.Name),
		Str("values"),
		Str("("),
		Str(PrintRandValues(vals)),
		Str(")"),
	)
})

var query = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
	cols := tbl.GetRandColumns()

	var commonSelect = NewFn(func(state *State) Fn {
		w := state.ctrl.Weight
		prepare := state.Search(ScopeKeyCurrentPrepare)
		if !prepare.IsNil() {
			paramCols := SwapOutParameterizedColumns(cols)
			prepare.ToPrepare().AppendColumns(paramCols...)
		}
		return And(Str("select"),
			OptIf(state.ctrl.EnableTestTiFlash,
				And(
					Str("/*+ read_from_storage(tiflash["),
					Str(tbl.Name),
					Str("]) */"),
				)),
			OptIf(w.Query_INDEX_MERGE,
				And(
					Str("/*+ use_index_merge("),
					Str(tbl.Name),
					Str(") */"),
				)),
			Str(PrintColumnNamesWithoutPar(cols, "*")),
			Str("from"),
			Str(tbl.Name),
			Str("where"),
			predicates,
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(tbl.Columns, "")),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		)
	})
	var forUpdateOpt = NewFn(func(state *State) Fn {
		return Opt(Str("for update"))
	})
	var union = NewFn(func(state *State) Fn {
		return Or(
			Str("union"),
			Str("union all"),
			Str("except"),
			Str("intersect"),
		)
	})
	var aggSelect = NewFn(func(state *State) Fn {
		var aggCols []*Column
		for i := 0; i < 5; i++ {
			aggCols = append(aggCols, tbl.GetRandColumn())
		}
		groupByCols := tbl.GetRandColumns()
		aggFunc := PrintRandomAggFunc(tbl, aggCols)
		state.ctrl.EnableAggPushDown = rand.Intn(2) == 1
		state.ctrl.AggType = []string{"", "hash_agg", "stream_agg"}[rand.Intn(3)]
		primaryKeyIdx := tbl.GetPrimaryKeyIndex()
		var primaryKeyCols []*Column
		if primaryKeyIdx != nil {
			primaryKeyCols = primaryKeyIdx.Columns
		}
		return And(
			Str("select"),
			Str("/*+"),
			OptIf(state.ctrl.EnableAggPushDown, Str("agg_to_cop()")),
			OptIf(state.ctrl.AggType != "", Str(state.ctrl.AggType+"()")),
			Str("*/"),
			Str(aggFunc),
			Str("from"),
			Str("(select"),
			OptIf(state.ctrl.EnableTestTiFlash,
				And(
					Str("/*+ read_from_storage(tiflash["),
					Str(tbl.Name),
					Str("]) */"),
				)),
			OptIf(w.Query_INDEX_MERGE,
				And(
					Str("/*+ use_index_merge("),
					Str(tbl.Name),
					Str(") */"),
				)),
			Str("*"),
			Str("from"),
			Str(tbl.Name),
			Str("where"),
			predicates,
			Str("order by"),
			OptIf(primaryKeyIdx != nil, Str(PrintColumnNamesWithoutPar(primaryKeyCols, ""))),
			OptIf(primaryKeyIdx == nil, Str("_tidb_rowid")),
			Str(") ordered_tbl"),
			OptIf(len(groupByCols) > 0, Str("group by")),
			OptIf(len(groupByCols) > 0, Str(PrintColumnNamesWithoutPar(groupByCols, ""))),
			OptIf(w.Query_HasOrderby > 0,
				Str("order by aggCol"),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		)
	})
	var windowSelect = NewFn(func(state *State) Fn {
		windowFunc := PrintRandomWindowFunc(tbl)
		window := PrintRandomWindow(tbl)
		return And(
			Str("select"),
			OptIf(state.ctrl.EnableTestTiFlash,
				And(
					Str("/*+ read_from_storage(tiflash["),
					Str(tbl.Name),
					Str("]) */"),
				)),
			OptIf(w.Query_INDEX_MERGE,
				And(
					Str("/*+ use_index_merge("),
					Str(tbl.Name),
					Str(") */"),
				)),
			Str(windowFunc),
			Str("over w"),
			Str("from"),
			Str(tbl.Name),
			Str("window w as"),
			Str(window),
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(tbl.Columns, "")),
					Str(", "),
					Str(windowFunc+" over w"),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		)
	})
	return Or(
		And(commonSelect, forUpdateOpt),
		And(
			Str("("), commonSelect, forUpdateOpt, Str(")"),
			union,
			Str("("), commonSelect, forUpdateOpt, Str(")"),
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(cols, "")),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		).SetW(w.Query_Union),
		And(aggSelect, forUpdateOpt),
		And(windowSelect, forUpdateOpt).SetW(w.Query_Window),
		And(
			Str("("), aggSelect, forUpdateOpt, Str(")"),
			union,
			Str("("), aggSelect, forUpdateOpt, Str(")"),
			OptIf(w.Query_HasOrderby > 0,
				Str("order by aggCol"),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		).SetW(w.Query_Union),
		And(
			Str("("), windowSelect, forUpdateOpt, Str(")"),
			union,
			Str("("), windowSelect, forUpdateOpt, Str(")"),
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by 1"),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		).SetW(w.Query_Window+w.Query_Union),
		If(len(state.tables) > 1,
			multiTableQuery,
		),
	)
})

var commonInsert = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := state.GetRandTable()
	var cols []*Column
	if state.ctrl.StrictTransTable {
		cols = tbl.GetRandColumnsIncludedDefaultValue()
	} else {
		cols = tbl.GetRandColumns()
	}
	insertOrReplace := "insert"
	if rand.Intn(3) == 0 && w.Query_DML_Can_Be_Replace {
		insertOrReplace = "replace"
	}

	var onDuplicateUpdate = NewFn(func(state *State) Fn {
		cols := tbl.GetRandColumnsNonEmpty()
		return Or(
			Empty().SetW(3),
			Strs(
				"on duplicate key update",
				PrintRandomAssignments(cols),
			).SetW(w.Query_DML_INSERT_ON_DUP),
		)
	})

	var multipleRowVals = NewFn(func(state *State) Fn {
		var rowVal = NewFn(func(state *State) Fn {
			vs := tbl.GenRandValues(cols)
			return Strs("(", PrintRandValues(vs), ")")
		})
		return RepeatRange(1, 7, rowVal, Str(","))
	})

	var insertSetStmt = NewFn(func(state *State) Fn {
		randCol := tbl.GetRandColumn()
		return Or(
			Strs(randCol.Name, "=", randCol.RandomValue()),
		)
	})

	// TODO: insert into t partition(p1) values(xxx)
	// TODO: insert ... select... , it's hard to make the selected columns match the inserted columns.
	return Or(
		And(
			Str(insertOrReplace),
			OptIf(insertOrReplace == "insert", Str("ignore")),
			Str("into"),
			Str(tbl.Name),
			Str(PrintColumnNamesWithPar(cols, "")),
			Str("values"),
			multipleRowVals,
			OptIf(insertOrReplace == "insert", onDuplicateUpdate),
		),
		And(
			Str(insertOrReplace),
			OptIf(insertOrReplace == "insert", Str("ignore")),
			Str("into"),
			Str(tbl.Name),
			Str("set"),
			RepeatRange(1, 3, insertSetStmt, Str(",")),
			OptIf(insertOrReplace == "insert", onDuplicateUpdate),
		),
		//And(
		//	Str(insertOrReplace),
		//	Opt(Str("ignore")),
		//	Str("into"),
		//	Str(tbl.Name),
		//	Str(PrintColumnNamesWithPar(cols, "")),
		//	commonSelect,
		//	OptIf(insertOrReplace == "insert", onDuplicateUpdate),
		//),
	)
})

var commonUpdate = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
	orderByCols := tbl.GetRandColumns()

	var updateAssignment = NewFn(func(state *State) Fn {
		randCol := tbl.GetRandColumn()
		return Or(
			Strs(randCol.Name, "=", randCol.RandomValue()),
		)
	})

	var updateAssignments = NewFn(func(state *State) Fn {
		return RepeatRange(1, 3, updateAssignment, Str(","))
	})

	return And(
		Str("update"),
		Str(tbl.Name),
		Str("set"),
		updateAssignments,
		Str("where"),
		predicates,
		OptIf(len(orderByCols) > 0,
			And(
				Str("order by"),
				Str(PrintColumnNamesWithoutPar(orderByCols, "")),
				maybeLimit,
			),
		),
	)
})

var commonAnalyze = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	return And(Str("analyze table"), Str(tbl.Name))
})

var commonDelete = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := state.GetRandTable()
	var col *Column
	if rand.Intn(w.Query_DML_DEL_COMMON+w.Query_DML_DEL_INDEX) < w.Query_DML_DEL_COMMON {
		col = tbl.GetRandColumn()
	} else {
		col = tbl.GetRandIndexFirstColumnWithWeight(w.Query_DML_DEL_INDEX_COMMON, w.Query_DML_DEL_INDEX_COMMON)
	}
	state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))

	var randRowVal = NewFn(func(state *State) Fn {
		return Str(col.RandomValue())
	})

	var multipleRowVal = NewFn(func(state *State) Fn {
		return RepeatRange(1, 9, randRowVal, Str(","))
	})

	return And(
		Str("delete from"),
		Str(tbl.Name),
		Str("where"),
		Or(
			And(predicates, maybeLimit),
			And(Str(col.Name), Str("in"), Str("("), multipleRowVal, Str(")"), maybeLimit),
			And(Str(col.Name), Str("is null"), maybeLimit),
		),
	)
})

var predicates = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := getTable(state)
	uniqueIdx := tbl.GetRandUniqueIndexForPointGet()

	if w.Query_INDEX_MERGE {
		andPredicates := NewFn(func(state *State) Fn {
			tbl := getTable(state)
			tbl.colForPrefixIndex = tbl.GetRandIndexPrefixColumn()
			repeatCnt := len(tbl.colForPrefixIndex)
			if repeatCnt == 0 {
				repeatCnt = 1
			}
			if rand.Intn(5) == 0 {
				repeatCnt += rand.Intn(2) + 1
			}
			return Repeat(predicate, repeatCnt, Str("and"))
		})

		// Give some chances to common predicate.
		if rand.Intn(5) != 0 {
			return RepeatRange(2, 5, andPredicates, Str("or"))
		}
	}

	var predicatesPointGet = NewFn(func(state *State) Fn {
		pointsCount := rand.Intn(4) + 1
		pointGetVals := make([][]string, pointsCount)
		for i := 0; i < pointsCount; i++ {
			if len(tbl.values) > 0 && RandomBool() {
				pointGetVals[i] = tbl.GetRandRow(uniqueIdx.Columns)
			} else {
				pointGetVals[i] = tbl.GenRandValues(uniqueIdx.Columns)
			}
		}
		return Or(
			Str(PrintPredicateDNF(uniqueIdx.Columns, pointGetVals)),
			Str(PrintPredicateCompoundDNF(uniqueIdx.Columns, pointGetVals)),
			Str(PrintPredicateIn(uniqueIdx.Columns, pointGetVals)),
		)
	})

	return Or(
		RepeatRange(1, 5, predicate, Or(Str("and"), Str("or"))).SetW(3),
		If(uniqueIdx != nil, predicatesPointGet).SetW(1),
	)
})

var predicate = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := getTable(state)
	randCol := tbl.GetRandColumn()
	if w.Query_INDEX_MERGE {
		if len(tbl.colForPrefixIndex) > 0 {
			randCol = tbl.colForPrefixIndex[0]
			tbl.colForPrefixIndex = tbl.colForPrefixIndex[1:]
		}
	}
	var randVal = NewFn(func(state *State) Fn {
		var v string
		prepare := state.Search(ScopeKeyCurrentPrepare)
		if !prepare.IsNil() && rand.Intn(50) == 0 {
			prepare.ToPrepare().AppendColumns(randCol)
			v = "?"
		} else if rand.Intn(3) == 0 || len(tbl.values) == 0 {
			v = randCol.RandomValue()
		} else {
			v = tbl.GetRandRowVal(randCol)
		}
		return Str(v)
	})
	var randColVals = NewFn(func(state *State) Fn {
		return RepeatRange(1, 5, randVal, Str(","))
	})
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	return Or(
		And(Str(colName), cmpSymbol, randVal),
		And(Str(colName), Str("in"), Str("("), randColVals, Str(")")),
	)
})

var cmpSymbol = NewFn(func(state *State) Fn {
	return Or(
		Str("="),
		Str("<"),
		Str("<="),
		Str(">"),
		Str(">="),
		Str("<>"),
		Str("!="),
	)
})

var maybeLimit = NewFn(func(state *State) Fn {
	// Return empty because the limit is not friendly to clustered index AB test.
	return Empty()
})

var addIndex = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	idx := GenNewIndex(state, state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
	tbl.AppendIndex(idx)

	return Strs(
		"alter table", tbl.Name,
		"add index", idx.Name,
		"(", PrintIndexColumnNames(idx), ")",
	)
})

var dropIndex = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		HasKey(ScopeKeyCurrentTable),
		HasIndices) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	idx := tbl.GetRandomIndex()
	tbl.RemoveIndex(idx)
	return Strs(
		"alter table", tbl.Name,
		"drop index", idx.Name,
	)
})

var addColumn = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	col := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	tbl.AppendColumn(col)
	return Strs(
		"alter table", tbl.Name,
		"add column", col.Name, PrintColumnType(col),
	)
})

var dropColumn = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		HasKey(ScopeKeyCurrentTable),
		MoreThan1Columns,
		HasDroppableColumn) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	col := tbl.GetRandDroppableColumn()
	tbl.RemoveColumn(col)
	return Strs(
		"alter table", tbl.Name,
		"drop column", col.Name,
	)
})

var alterColumn = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		HasKey(ScopeKeyCurrentTable),
		EnableColumnTypeChange) {
		return None()
	}
	return Or(
		alterColumnModify,
		alterColumnChange,
	)
})

var alterColumnModify = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		HasKey(ScopeKeyCurrentTable),
		EnableColumnTypeChange) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	col := tbl.GetRandColumn()
	newCol := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	newCol.Name = col.Name
	tbl.ReplaceColumn(col, newCol)
	return Strs("alter table", tbl.Name, "modify column", col.Name, PrintColumnType(newCol))
})

var alterColumnChange = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		HasKey(ScopeKeyCurrentTable),
		EnableColumnTypeChange) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTable).ToTable()
	col := tbl.GetRandColumn()
	newCol := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	tbl.ReplaceColumn(col, newCol)
	return Strs("alter table", tbl.Name, "change column", col.Name, newCol.Name, PrintColumnType(newCol))
})

var multiTableQuery = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl1 := state.GetRandTable()
	tbl2 := state.GetRandTable()
	cols1 := tbl1.GetRandColumns()
	cols2 := tbl2.GetRandColumns()
	state.Store(ScopeKeyCurrentMultiTable, NewScopeObj([]*Table{tbl1, tbl2}))
	preferIndex := RandomBool()

	var joinPredicate = NewFn(func(state *State) Fn {
		var col1, col2 *Column
		if preferIndex {
			col1 = tbl1.GetRandColumnsPreferIndex()
			col2 = tbl2.GetRandColumnsPreferIndex()
		} else {
			col1 = tbl1.GetRandColumnsSimple()
			col2 = tbl2.GetRandColumnsSimple()
		}
		return And(
			Str(col1.Name),
			cmpSymbol,
			Str(col2.Name),
		)
	})

	var joinPredicates = NewFn(func(state *State) Fn {
		sep := Or(Str("and"), Str("or"))
		return RepeatRange(1, 5, joinPredicate, sep)
	})

	var joinHint = NewFn(func(state *State) Fn {
		noIndexHint := Or(
			And(
				Str("MERGE_JOIN("),
				Str(tbl1.Name),
				Str(","),
				Str(tbl2.Name),
				Str(")"),
			),
			And(
				Str("HASH_JOIN("),
				Str(tbl1.Name),
				Str(","),
				Str(tbl2.Name),
				Str(")"),
			),
		)

		useIndexHint := Or(
			And(
				Str("INL_JOIN("),
				Str(tbl1.Name),
				Str(","),
				Str(tbl2.Name),
				Str(")"),
			),
			And(
				Str("INL_HASH_JOIN("),
				Str(tbl1.Name),
				Str(","),
				Str(tbl2.Name),
				Str(")"),
			),
			And(
				Str("INL_MERGE_JOIN("),
				Str(tbl1.Name),
				Str(","),
				Str(tbl2.Name),
				Str(")"),
			),
		)
		if preferIndex {
			return Or(
				Empty(),
				useIndexHint,
			)
		}

		return Or(
			Empty(),
			noIndexHint,
		)
	})

	var semiJoinStmt = NewFn(func(state *State) Fn {
		var col1, col2 *Column
		if preferIndex {
			col1 = tbl1.GetRandColumnsPreferIndex()
			col2 = tbl2.GetRandColumnsPreferIndex()
		} else {
			col1 = tbl1.GetRandColumnsSimple()
			col2 = tbl2.GetRandColumnsSimple()
		}

		// We prefer that the types to be compatible.
		// This method may be extracted to a function later.
		for i := 0; i <= 5; i++ {
			if col1.Tp.IsStringType() && col2.Tp.IsStringType() {
				break
			}
			if col1.Tp.IsIntegerType() && col2.Tp.IsIntegerType() {
				break
			}
			if col1.Tp == col2.Tp {
				break
			}
			if preferIndex {
				col2 = tbl2.GetRandColumnsPreferIndex()
			} else {
				col2 = tbl2.GetRandColumnsSimple()
			}
		}

		// TODO: Support exists subquery.
		return And(
			Str("select"),
			And(
				Str("/*+ "),
				OptIf(state.ctrl.EnableTestTiFlash,
					And(
						Str("read_from_storage(tiflash["),
						Str(tbl1.Name),
						Str(","),
						Str(tbl2.Name),
						Str("])"),
					)),
				joinHint,
				Str(" */"),
			),
			Str(PrintFullQualifiedColName(tbl1, cols1)),
			Str("from"),
			Str(tbl1.Name),
			Str("where"),
			Str(col1.Name),
			Str("in"),
			Str("("),
			Str("select"),
			Str(col2.Name),
			Str("from"),
			Str(tbl2.Name),
			Str("where"),
			predicates,
			Str(")"),
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(tbl1.Columns, "")),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		)
	})

	return Or(
		And(
			Str("select"),
			And(
				Str("/*+ "),
				OptIf(state.ctrl.EnableTestTiFlash,
					And(
						Str("read_from_storage(tiflash["),
						Str(tbl1.Name),
						Str(","),
						Str(tbl2.Name),
						Str("])"),
					)),
				joinHint,
				Str(" */"),
			),
			Str(PrintFullQualifiedColName(tbl1, cols1)),
			Str(","),
			Str(PrintFullQualifiedColName(tbl2, cols2)),
			Str("from"),
			Str(tbl1.Name),
			Or(Str("left join"), Str("join"), Str("right join")),
			Str(tbl2.Name),
			And(Str("on"), joinPredicates),
			And(Str("where")),
			predicates,
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(tbl1.Columns, "")),
					Str(","),
					Str(PrintColumnNamesWithoutPar(tbl2.Columns, "")),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		),
		And(
			Str("select"),
			And(
				Str("/*+ "),
				OptIf(state.ctrl.EnableTestTiFlash,
					And(
						Str("read_from_storage(tiflash["),
						Str(tbl1.Name),
						Str(","),
						Str(tbl2.Name),
						Str("])"),
					)),
				OptIf(w.Query_INDEX_MERGE,
					And(
						Str("use_index_merge("),
						Str(tbl1.Name),
						Str(","),
						Str(tbl2.Name),
						Str(")"),
					)),
				joinHint,
				Str(" */"),
			),
			Str(PrintFullQualifiedColName(tbl1, cols1)),
			Str(","),
			Str(PrintFullQualifiedColName(tbl2, cols2)),
			Str("from"),
			Str(tbl1.Name),
			Str("join"),
			Str(tbl2.Name),
			OptIf(w.Query_HasOrderby > 0,
				And(
					Str("order by"),
					Str(PrintColumnNamesWithoutPar(tbl1.Columns, "")),
					Str(","),
					Str(PrintColumnNamesWithoutPar(tbl2.Columns, "")),
				),
			),
			OptIf(w.Query_HasLimit > 0,
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		),
		semiJoinStmt,
	)
})

var createTableLike = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(NoTooMuchTables) {
		return None()
	}
	tbl := state.GetRandTable()
	newTbl := tbl.Clone(func() int {
		return state.AllocGlobalID(ScopeKeyTableUniqID)
	}, func() int {
		return state.AllocGlobalID(ScopeKeyColumnUniqID)
	}, func() int {
		return state.AllocGlobalID(ScopeKeyIndexUniqID)
	})
	state.AppendTable(newTbl)
	return Strs("create table", newTbl.Name, "like", tbl.Name)
})

var selectIntoOutFile = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, EnabledSelectIntoAndLoad) {
		return None()
	}
	tbl := state.GetRandTable()
	state.StoreInRoot(ScopeKeyLastOutFileTable, NewScopeObj(tbl))
	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, state.AllocGlobalID(ScopeKeyTmpFileID)))
	return Strs("select * from", tbl.Name, "into outfile", fmt.Sprintf("'%s'", tmpFile))
})

var loadTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, EnabledSelectIntoAndLoad, AlreadySelectOutfile) {
		return None()
	}
	tbl := state.Search(ScopeKeyLastOutFileTable).ToTable()
	id := state.Search(ScopeKeyTmpFileID).ToInt()
	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, id))
	randChildTable := tbl.childTables[rand.Intn(len(tbl.childTables))]
	return Strs("load data local infile", fmt.Sprintf("'%s'", tmpFile), "into table", randChildTable.Name)
})

var splitRegion = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
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

var prepareStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	prepare := GenNewPrepare(state.AllocGlobalID(ScopeKeyPrepareID))
	state.AppendPrepare(prepare)
	state.Store(ScopeKeyCurrentPrepare, NewScopeObj(prepare))
	return And(
		Str("prepare"),
		Str(prepare.Name),
		Str("from"),
		Str(`"`),
		query,
		Str(`"`))
})

var deallocPrepareStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, HasPreparedStmts) {
		return None()
	}
	prepare := state.GetRandPrepare()
	state.RemovePrepare(prepare)
	return Strs("deallocate prepare", prepare.Name)
})

var queryPrepare = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, HasPreparedStmts) {
		return None()
	}
	Assert(len(state.prepareStmts) > 0, state)
	prepare := state.GetRandPrepare()
	assignments := prepare.GenAssignments()
	if len(assignments) == 0 {
		return Str(fmt.Sprintf("execute %s", prepare.Name))
	}
	for i := 1; i < len(assignments); i++ {
		state.InjectTodoSQL(assignments[i])
	}
	userVarsStr := strings.Join(prepare.UserVars(), ",")
	state.InjectTodoSQL(fmt.Sprintf("execute %s using %s", prepare.Name, userVarsStr))
	return Str(assignments[0])
})

func RunInteractTest(ctx context.Context, db1, db2 *sql.DB, state *State, sql string) error {
	return runInteractTest(ctx, db1, db2, state, sql, true)
}

// RunInteractTestNoSort is similar to RunInteractTest, but RunInteractTestNoSort doesn't sort the query results
// before compare them. It'll be useful to run tests for SQLs with "order by" clause.
func RunInteractTestNoSort(ctx context.Context, db1, db2 *sql.DB, state *State, sql string) error {
	return runInteractTest(ctx, db1, db2, state, sql, false)
}

func runInteractTest(ctx context.Context, db1, db2 *sql.DB, state *State, sql string, sortQueryResult bool) error {
	log.Printf("%s", sql)
	lsql := strings.ToLower(sql)
	isAdminCheck := strings.Contains(lsql, "admin") && strings.Contains(lsql, "check")
	rs1, err1 := runQuery(ctx, db1, sql)
	rs2, err2 := runQuery(ctx, db2, sql)
	if isAdminCheck && err1 != nil && !strings.Contains(err1.Error(), "t exist") {
		return err1
	}
	if isAdminCheck && err2 != nil && !strings.Contains(err2.Error(), "t exist") {
		return err2
	}
	if !ValidateErrs(err1, err2) {
		return fmt.Errorf("errors mismatch: %v <> %v %q", err1, err2, sql)
	}
	if rs1 == nil || rs2 == nil {
		return nil
	}
	var h1, h2 string
	if sortQueryResult {
		h1, h2 = rs1.OrderedDigest(resultset.DigestOptions{}), rs2.OrderedDigest(resultset.DigestOptions{})
	} else {
		h1, h2 = rs1.DataDigest(resultset.DigestOptions{}), rs2.DataDigest(resultset.DigestOptions{})
	}
	if h1 != h2 {
		var b1, b2 bytes.Buffer
		rs1.PrettyPrint(&b1)
		rs2.PrettyPrint(&b2)
		return fmt.Errorf("result digests mismatch: %s != %s %q\n%s\n%s", h1, h2, sql, b1.String(), b2.String())
	}
	if rs1.IsExecResult() && rs1.ExecResult().RowsAffected != rs2.ExecResult().RowsAffected {
		return fmt.Errorf("rows affected mismatch: %d != %d %q",
			rs1.ExecResult().RowsAffected, rs2.ExecResult().RowsAffected, sql)
	}
	return nil
}

func runQuery(ctx context.Context, db *sql.DB, sql string) (*resultset.ResultSet, error) {
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return resultset.ReadFromRows(rows)
}

func ValidateErrs(err1 error, err2 error) bool {
	ignoreErrMsgs := []string{
		"with index covered now",                         // 4.0 cannot drop column with index
		"Unknown system variable",                        // 4.0 cannot recognize tidb_enable_clustered_index
		"Split table region lower value count should be", // 4.0 not compatible with 'split table between'
		"Column count doesn't match value count",         // 4.0 not compatible with 'split table by'
		"for column '_tidb_rowid'",                       // 4.0 split table between may generate incorrect value.
		"Unknown column '_tidb_rowid'",                   // 5.0 clustered index table don't have _tidb_row_id.
	}
	for _, msg := range ignoreErrMsgs {
		match := OneOfContains(err1, err2, msg)
		if match {
			return true
		}
	}
	return (err1 == nil && err2 == nil) || (err1 != nil && err2 != nil)
}

func OneOfContains(err1, err2 error, msg string) bool {
	c1 := err1 != nil && strings.Contains(err1.Error(), msg) && err2 == nil
	c2 := err2 != nil && strings.Contains(err2.Error(), msg) && err1 == nil
	return c1 || c2
}

func getTable(state *State) *Table {
	var tbl *Table
	inMultiTableQuery := !state.Search(ScopeKeyCurrentMultiTable).IsNil()
	if inMultiTableQuery {
		tables := state.Search(ScopeKeyCurrentMultiTable).ToTables()
		if RandomBool() {
			tbl = tables[0]
		} else {
			tbl = tables[1]
		}
	} else {
		tbl = state.Search(ScopeKeyCurrentTable).ToTable()
	}
	return tbl
}
