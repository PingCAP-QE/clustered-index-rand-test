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

	"github.com/cznic/mathutil"
	"github.com/zyguan/sqlz/resultset"
)

func NewGenerator(state *State) func() string {
	rand.Seed(time.Now().UnixNano())
	return func() string {
		return Start.Eval(state)
	}
}

var Start = NewFn(func(state *State) Fn {
	if s, ok := state.PopOneTodoSQL(); ok {
		return Str(s)
	}
	if !state.Initialized() {
		eInitStart := Str(InitStart.Eval(state))
		if state.MeetInitializedDemand() {
			state.SetInitialized()
		}
		return eInitStart
	}
	return Or(
		SwitchRowFormatVer.SetW(1),
		SwitchClustered.SetW(1),
		AdminCheck.SetW(1),
		CreateTable.SetW(13),
		CreateTableLike.SetW(13),
		DMLStmt.SetW(20),
		DDLStmt.SetW(5),
		SplitRegion.SetW(1),
		AnalyzeTable.SetW(0),
		PrepareStmt.SetW(2),
		DeallocPrepareStmt.SetW(1),
		FlashBackTable.SetW(1),
		SelectIntoOutFile.SetW(1),
		LoadTable.SetW(1),
		DropTable.SetW(1),
	)
})

var InitStart = NewFn(func(state *State) Fn {
	Assert(!state.Initialized())
	if len(state.tables) < state.ctrl.InitTableCount {
		return CreateTable
	} else {
		return InsertInto
	}
})

var DMLStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	return Or(
		Query.SetW(1),
		QueryPrepare.SetW(1),
		CommonDelete.SetW(1),
		CommonInsertOrReplace.SetW(1),
		CommonUpdate.SetW(1),
	)
})

var DDLStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl}))
	return Or(
		AddColumn,
		AddIndex,
		DropColumn,
		DropIndex,
		AlterColumn,
	)
})

var SwitchRowFormatVer = NewFn(func(state *State) Fn {
	if RandomBool() {
		return Str("set @@global.tidb_row_format_version = 2")
	}
	return Str("set @@global.tidb_row_format_version = 1")
})

var SwitchClustered = NewFn(func(state *State) Fn {
	if RandomBool() {
		return Str("set @@global.tidb_enable_clustered_index = 0")
	}
	return Str("set @@global.tidb_enable_clustered_index = 1")
})

var DropTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	return Strs("drop table", tbl.Name)
})

var FlashBackTable = NewFn(func(state *State) Fn {
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

var AdminCheck = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
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

var CreateTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(NoTooMuchTables) {
		return None()
	}
	tbl := GenNewTable(state.AllocGlobalID(ScopeKeyTableUniqID))
	state.AppendTable(tbl)
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl}))
	if state.ctrl.EnableTestTiFlash {
		state.InjectTodoSQL(fmt.Sprintf("alter table %s set tiflash replica 1", tbl.Name))
		state.InjectTodoSQL(fmt.Sprintf("select sleep(20)"))
	}
	// The eval order matters because the dependency is ColumnDefinitions <- PartitionDefinition <- IndexDefinitions.
	eColDefs := ColumnDefinitions.Eval(state)
	partCol := tbl.GetRandColumnForPartition()
	if partCol != nil {
		state.Store(ScopeKeyCurrentPartitionColumn, NewScopeObj(partCol))
	}
	ePartitionDef := PartitionDefinition.Eval(state)
	eIdxDefs := IndexDefinitions.Eval(state)
	return Strs("create table", tbl.Name, "(", eColDefs, eIdxDefs, ")", ePartitionDef)
})

var ColumnDefinitions = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	if !state.Initialized() {
		return Repeat(ColumnDefinition, state.ctrl.InitColCount, Str(","))
	}
	return RepeatRange(1, 10, ColumnDefinition, Str(","))
})

var ColumnDefinition = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	tbl.AppendColumn(col)
	return And(Str(col.Name), Str(PrintColumnType(col)))
})

var IndexDefinitions = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	if RandomBool() {
		return Empty()
	}
	return And(
		Str(","),
		RepeatRange(1, 4, IndexDefinition, Str(",")),
	)
})

var IndexDefinition = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
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

var PartitionDefinition = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	if !state.Exists(ScopeKeyCurrentPartitionColumn) {
		return Empty()
	}
	partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
	tbl.AppendPartitionColumn(partitionedCol)
	return Or(
		Empty(),
		PartitionDefinitionHash,
		PartitionDefinitionRange,
		PartitionDefinitionList,
	)
})

var PartitionDefinitionHash = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentPartitionColumn)) {
		return None()
	}
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

var PartitionDefinitionRange = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentPartitionColumn)) {
		return None()
	}
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

var PartitionDefinitionList = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentPartitionColumn)) {
		return None()
	}
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

var InsertInto = NewFn(func(state *State) Fn {
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

var Query = NewFn(func(state *State) Fn {
	return Or(
		SingleSelect,
		UnionSelect,
		AggSelect,
		WindowSelect,
		MultiTableQuery,
		And(
			Str("("), AggSelect, Str(")"),
			SetOperator,
			Str("("), AggSelect, Str(")"),
			Opt(
				Str("order by aggCol"),
			),
			Opt(
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		),
		And(
			Str("("), WindowSelect, ForUpdateOpt, Str(")"),
			SetOperator,
			Str("("), WindowSelect, ForUpdateOpt, Str(")"),
			Opt(
				And(
					Str("order by 1"),
				),
			),
			Opt(
				And(
					Str("limit"),
					Str(RandomNum(1, 1000)),
				),
			),
		),
	)
})

var AggSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbl := state.GetRandTable()
	var aggCols []*Column
	for i := 0; i < 5; i++ {
		aggCols = append(aggCols, tbl.GetRandColumn())
	}
	groupByCols := tbl.GetRandColumns()
	aggFunc := PrintRandomAggFunc(tbl, aggCols)
	primaryKeyIdx := tbl.GetPrimaryKeyIndex()
	var primaryKeyCols []*Column
	if primaryKeyIdx != nil {
		primaryKeyCols = primaryKeyIdx.Columns
	}
	return And(
		Str("select"),
		HintAggToCop,
		Str(aggFunc),
		Str("from"),
		Str("(select"),
		HintTiFlash,
		HintIndexMerge,
		Str("*"),
		Str("from"),
		Str(tbl.Name),
		Str("where"),
		Predicates,
		Str("order by"),
		OptIf(primaryKeyIdx != nil, Str(PrintColumnNamesWithoutPar(primaryKeyCols, ""))),
		OptIf(primaryKeyIdx == nil, Str("_tidb_rowid")),
		Str(") ordered_tbl"),
		OptIf(len(groupByCols) > 0, Str("group by")),
		OptIf(len(groupByCols) > 0, Str(PrintColumnNamesWithoutPar(groupByCols, ""))),
		Opt(
			Str("order by aggCol"),
		),
		Opt(
			And(
				Str("limit"),
				Str(RandomNum(1, 1000)),
			),
		),
		ForUpdateOpt,
	)
})

var WindowSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	windowFunc := PrintRandomWindowFunc(tbl)
	window := PrintRandomWindow(tbl)
	return And(
		Str("select"), HintTiFlash, HintIndexMerge,
		Str(windowFunc),
		Str("over w"),
		Str("from"),
		Str(tbl.Name),
		Str("window w as"),
		Str(window),
		Opt(
			And(
				Str("order by"),
				Str(PrintColumnNamesWithoutPar(tbl.Columns, "")),
				Str(", "),
				Str(windowFunc+" over w"),
			),
		),
		Opt(
			And(
				Str("limit"),
				Str(RandomNum(1, 1000)),
			),
		),
		ForUpdateOpt,
	)
})

var SingleSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl}))
	state.Store(ScopeKeyCurrentSelectedColNum, NewScopeObj(1+rand.Intn(len(tbl.Columns))))
	return CommonSelect
})

var UnionSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl1, tbl2 := state.GetRandTable(), state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl1, tbl2}))
	colLen := mathutil.Min(len(tbl1.Columns), len(tbl2.Columns))
	state.Store(ScopeKeyCurrentSelectedColNum, NewScopeObj(1+rand.Intn(colLen)))
	return And(
		Str("("), CommonSelect, Str(")"),
		SetOperator,
		Str("("), CommonSelect, Str(")"),
	)
})

var CommonSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables),
		MustHaveKey(ScopeKeyCurrentSelectedColNum)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	cols := tbl.GetRandNColumns(state.Search(ScopeKeyCurrentSelectedColNum).ToInt())
	if state.Exists(ScopeKeyCurrentPrepare) {
		paramCols := SwapOutParameterizedColumns(cols)
		prepare := state.Search(ScopeKeyCurrentPrepare).ToPrepare()
		prepare.AppendColumns(paramCols...)
	}
	return And(Str("select"), HintTiFlash, HintIndexMerge,
		Str(PrintColumnNamesWithoutPar(cols, "*")),
		Str("from"), Str(tbl.Name), Str("where"),
		Predicates, OrderByLimit, ForUpdateOpt,
	)
})

var ForUpdateOpt = NewFn(func(state *State) Fn {
	return Opt(Str("for update"))
})

var HintTiFlash = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	return OptIf(state.ctrl.EnableTestTiFlash,
		Strs("/*+ read_from_storage(tiflash[", PrintTableNames(tbls), "]) */"),
	)
})

var HintIndexMerge = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	return OptIf(state.ExistsConfig(ConfigKeyUnitIndexMergeHint),
		Strs("/*+ use_index_merge(", PrintTableNames(tbls), ") */"),
	)
})

var HintAggToCop = NewFn(func(state *State) Fn {
	return And(
		Str("/*+"),
		Opt(Str("agg_to_cop()")),
		Or(Empty(), Str("hash_agg()"), Str("stream_agg()")),
		Str("*/"),
	)
})

var SetOperator = NewFn(func(state *State) Fn {
	return Or(
		Str("union"),
		Str("union all"),
		Str("except"),
		Str("intersect"),
	)
})

var OrderByLimit = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	switch state.SearchConfig(ConfigKeyEnumLimitOrderBy).ToString() {
	case ConfigKeyEnumLOBNone:
		return Empty()
	case ConfigKeyEnumLOBOrderBy:
		return Strs("order by", PrintQualifiedColumnNames(tbls))
	case ConfigKeyEnumLOBLimitOrderBy:
		return Strs("order by", PrintQualifiedColumnNames(tbls), "limit", RandomNum(1, 1000))
	default:
		NeverReach()
		return Empty()
	}
})

var CommonInsertOrReplace = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl}))
	var cols []*Column
	if state.ctrl.StrictTransTable {
		cols = tbl.GetRandColumnsIncludedDefaultValue()
	} else {
		cols = tbl.GetRandColumns()
	}
	state.Store(ScopeKeyCurrentSelectedColumns, NewScopeObj(cols))
	// TODO: insert into t partition(p1) values(xxx)
	// TODO: insert ... select... , it's hard to make the selected columns match the inserted columns.
	return Or(
		CommonInsertValues,
		CommonInsertSet,
		CommonReplaceValues,
		CommonReplaceSet,
	)
})

var CommonInsertSet = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	return And(
		Str("insert into"), Opt(Str("ignore")), Str(tbl.Name),
		Str(PrintColumnNamesWithPar(cols, "")),
		Str("set"),
		RepeatRange(1, 3, InsertSetStmt, Str(",")),
		Opt(OnDuplicateUpdate),
	)
})

var CommonInsertValues = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	return And(
		Str("insert into"), Opt(Str("ignore")), Str(tbl.Name),
		Str(PrintColumnNamesWithPar(cols, "")),
		Str("values"),
		MultipleRowVals,
		Opt(OnDuplicateUpdate),
	)
})

var CommonReplaceValues = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	return And(
		Str("replace into"), Str(tbl.Name),
		Str(PrintColumnNamesWithPar(cols, "")),
		Str("values"),
		MultipleRowVals,
	)
})

var CommonReplaceSet = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	return And(
		Str("replace into"), Str(tbl.Name),
		Str(PrintColumnNamesWithPar(cols, "")),
		Str("set"),
		RepeatRange(1, 3, InsertSetStmt, Str(",")),
	)
})

var MultipleRowVals = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	var rowVal = NewFn(func(state *State) Fn {
		vs := tbl.GenRandValues(cols)
		return Strs("(", PrintRandValues(vs), ")")
	})
	return RepeatRange(1, 7, rowVal, Str(","))
})

var InsertSetStmt = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	randCol := tbl.GetRandColumn()
	return Or(
		Strs(randCol.Name, "=", randCol.RandomValue()),
	)
})

var OnDuplicateUpdate = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := tbl.GetRandColumnsNonEmpty()
	return Strs(
		"on duplicate key update",
		PrintRandomAssignments(cols),
	)
})

var CommonUpdate = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl}))
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
		Predicates,
		OptIf(len(orderByCols) > 0,
			And(
				Str("order by"),
				Str(PrintColumnNamesWithoutPar(orderByCols, "")),
				MaybeLimit,
			),
		),
	)
})

var AnalyzeTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None()
	}
	tbl := state.GetRandTable()
	return And(Str("analyze table"), Str(tbl.Name))
})

var CommonDelete = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := state.GetRandTable()
	var col *Column
	if rand.Intn(w.Query_DML_DEL_COMMON+w.Query_DML_DEL_INDEX) < w.Query_DML_DEL_COMMON {
		col = tbl.GetRandColumn()
	} else {
		col = tbl.GetRandIndexFirstColumnWithWeight(w.Query_DML_DEL_INDEX_COMMON, w.Query_DML_DEL_INDEX_COMMON)
	}
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl}))

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
			And(Predicates, MaybeLimit),
			And(Str(col.Name), Str("in"), Str("("), multipleRowVal, Str(")"), MaybeLimit),
			And(Str(col.Name), Str("is null"), MaybeLimit),
		),
	)
})

var Predicates = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	uniqueIdx := tbl.GetRandUniqueIndexForPointGet()
	if uniqueIdx != nil {
		state.Store(ScopeKeyCurrentUniqueIndexForPointGet, NewScopeObj(uniqueIdx))
	}
	if w.Query_INDEX_MERGE {
		andPredicates := NewFn(func(state *State) Fn {
			tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
			tbl.colForPrefixIndex = tbl.GetRandIndexPrefixColumn()
			repeatCnt := len(tbl.colForPrefixIndex)
			if repeatCnt == 0 {
				repeatCnt = 1
			}
			if rand.Intn(5) == 0 {
				repeatCnt += rand.Intn(2) + 1
			}
			return Repeat(Predicate, repeatCnt, Str("and"))
		})

		// Give some chances to common predicate.
		if rand.Intn(5) != 0 {
			return RepeatRange(2, 5, andPredicates, Str("or"))
		}
	}

	return Or(
		RepeatRange(1, 5, Predicate, Or(Str("and"), Str("or"))).SetW(3),
		PredicatesPointGet.SetW(1),
	)
})

var PredicatesPointGet = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		HasKey(ScopeKeyCurrentUniqueIndexForPointGet)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	uniqueIdx := state.Search(ScopeKeyCurrentUniqueIndexForPointGet).ToIndex()
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

var Predicate = NewFn(func(state *State) Fn {
	w := state.ctrl.Weight
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
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
		And(Str(colName), CompareSymbol, randVal),
		And(Str(colName), Str("in"), Str("("), randColVals, Str(")")),
	)
})

var CompareSymbol = NewFn(func(state *State) Fn {
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

var MaybeLimit = NewFn(func(state *State) Fn {
	// Return empty because the limit is not friendly to clustered index AB test.
	return Empty()
})

var AddIndex = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables)) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	idx := GenNewIndex(state, state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
	tbl.AppendIndex(idx)

	return Strs(
		"alter table", tbl.Name,
		"add index", idx.Name,
		"(", PrintIndexColumnNames(idx), ")",
	)
})

var DropIndex = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables),
		HasIndices) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	idx := tbl.GetRandomIndex()
	tbl.RemoveIndex(idx)
	return Strs(
		"alter table", tbl.Name,
		"drop index", idx.Name,
	)
})

var AddColumn = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	tbl.AppendColumn(col)
	return Strs(
		"alter table", tbl.Name,
		"add column", col.Name, PrintColumnType(col),
	)
})

var DropColumn = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables),
		MoreThan1Columns,
		HasDroppableColumn) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := tbl.GetRandDroppableColumn()
	tbl.RemoveColumn(col)
	return Strs(
		"alter table", tbl.Name,
		"drop column", col.Name,
	)
})

var AlterColumn = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables),
		EnableColumnTypeChange) {
		return None()
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := tbl.GetRandColumn()
	newCol := GenNewColumn(state, state.AllocGlobalID(ScopeKeyColumnUniqID))
	tbl.ReplaceColumn(col, newCol)
	if RandomBool() {
		newCol.Name = col.Name
		return Strs("alter table", tbl.Name, "modify column", col.Name, PrintColumnType(newCol))
	}
	return Strs("alter table", tbl.Name, "change column", col.Name, newCol.Name, PrintColumnType(newCol))
})

var MultiTableQuery = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasAtLeast2Tables) {
		return None()
	}
	w := state.ctrl.Weight
	tbl1 := state.GetRandTable()
	tbl2 := state.GetRandTable()
	cols1 := tbl1.GetRandColumns()
	cols2 := tbl2.GetRandColumns()
	state.Store(ScopeKeyCurrentTables, NewScopeObj(Tables{tbl1, tbl2}))
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
			CompareSymbol,
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
			Str("select"), HintTiFlash,
			And(
				Str("/*+ "),
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
			Predicates,
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
			Str("select"), HintTiFlash,
			And(
				Str("/*+ "),
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
			Predicates,
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
			Str("select"), HintTiFlash, HintIndexMerge,
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

var CreateTableLike = NewFn(func(state *State) Fn {
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

var SelectIntoOutFile = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, EnabledSelectIntoAndLoad) {
		return None()
	}
	tbl := state.GetRandTable()
	state.StoreInRoot(ScopeKeyLastOutFileTable, NewScopeObj(tbl))
	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, state.AllocGlobalID(ScopeKeyTmpFileID)))
	return Strs("select * from", tbl.Name, "into outfile", fmt.Sprintf("'%s'", tmpFile))
})

var LoadTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, EnabledSelectIntoAndLoad, AlreadySelectOutfile) {
		return None()
	}
	tbl := state.Search(ScopeKeyLastOutFileTable).ToTable()
	id := state.Search(ScopeKeyTmpFileID).ToInt()
	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, id))
	randChildTable := tbl.childTables[rand.Intn(len(tbl.childTables))]
	return Strs("load data local infile", fmt.Sprintf("'%s'", tmpFile), "into table", randChildTable.Name)
})

var SplitRegion = NewFn(func(state *State) Fn {
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

var PrepareStmt = NewFn(func(state *State) Fn {
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
		Query,
		Str(`"`))
})

var DeallocPrepareStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, HasPreparedStmts) {
		return None()
	}
	prepare := state.GetRandPrepare()
	state.RemovePrepare(prepare)
	return Strs("deallocate prepare", prepare.Name)
})

var QueryPrepare = NewFn(func(state *State) Fn {
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
