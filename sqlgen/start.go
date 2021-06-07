package sqlgen

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"strings"

	"github.com/cznic/mathutil"
)

var Start = NewFn(func(state *State) Fn {
	if s, ok := state.PopOneTodoSQL(); ok {
		return Str(s)
	}
	return Or(
		SwitchRowFormatVer.SetW(1),
		SwitchClustered.SetW(1),
		AdminCheck.SetW(1),
		CreateTable.SetW(13),
		CreateTableLike.SetW(6),
		Query.SetW(20),
		QueryPrepare.SetW(2),
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

var DMLStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	return Or(
		CommonDelete.SetW(1),
		CommonInsertOrReplace.SetW(3),
		CommonUpdate.SetW(1),
	)
})

var DDLStmt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
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
		return None
	}
	tbl := state.GetRandTable()
	state.RemoveTable(tbl)
	return Strs("drop table", tbl.Name)
})

var FlashBackTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	state.InjectTodoSQL(fmt.Sprintf("/*DDL*/ flashback table %s", tbl.Name))
	return Or(
		Strs("drop table", tbl.Name),
		Strs("truncate table", tbl.Name),
	)
})

var AdminCheck = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	if len(tbl.Indices) == 0 {
		return Strs("admin check table", tbl.Name)
	}
	idx := tbl.GetRandomIndex()
	return Or(
		Strs("admin check table", tbl.Name),
		Strs("admin check index", tbl.Name, idx.Name),
	)
})

var CreateTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(NoTooMuchTables) {
		return None
	}
	tbl := state.GenNewTable()
	state.AppendTable(tbl)
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
	if state.Roll(ConfigKeyProbabilityTiFlashTable, 0) {
		state.InjectTodoSQL(fmt.Sprintf("/*DDL*/ alter table %s set tiflash replica 1", tbl.Name))
		state.InjectTodoSQL(fmt.Sprintf("select sleep(20)"))
	}
	// The eval order matters because the dependency is ColumnDefinitions <- PartitionDefinition <- IndexDefinitions.
	eColDefs := ColumnDefinitions.Eval(state)
	partCol := tbl.GetRandColumnForPartition()
	if partCol != nil {
		state.Store(ScopeKeyCurrentPartitionColumn, partCol)
	}
	ePartitionDef := PartitionDefinition.Eval(state)
	eTableOption := TableOptions.Eval(state)
	eIdxDefs := IndexDefinitions.Eval(state)
	if eIdxDefs == "" {
		return Strs("create table", tbl.Name, "(", eColDefs, ")",
			eTableOption, ePartitionDef)
	} else {
		return Strs("create table", tbl.Name, "(", eColDefs, ",", eIdxDefs, ")",
			eTableOption, ePartitionDef)
	}
})

var TableOptions = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	return Strs("collate", tbl.Collate.String())
})

var ColumnDefinitions = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	return Repeat(ColumnDefinition.SetR(1, 10), Str(","))
})

var ColumnDefinition = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	tps := state.SearchConfig(ConfigKeyArrayAllowColumnTypes).
		ToConfigAllowedColumnTypes().CreateTableOrDefault()
	col := state.GenNewColumnWithType(tps...)
	tbl.AppendColumn(col)
	return And(Str(col.Name), Str(PrintColumnType(col)))
})

var IndexDefinitions = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	return Repeat(IndexDefinition.SetR(0, 4), Str(","))
})

var IndexDefinition = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	idx := state.GenNewIndex(tbl)
	if idx == nil {
		// TODO: add warning log.
		return Empty
	}
	tbl.AppendIndex(idx)
	return And(
		Str(PrintIndexType(idx)), Str("key"), Str(idx.Name),
		Str("("), Str(PrintIndexColumnNames(idx)), Str(")"),
		If(idx.Tp == IndexTypePrimary && state.ExistsConfig(ConfigKeyUnitPKNeedClusteredHint),
			Str("/*T![clustered_index] clustered */"),
		),
	)
})

var PartitionDefinition = NewFn(func(state *State) Fn {
	if !state.Exists(ScopeKeyCurrentPartitionColumn) {
		return Empty
	}
	return Or(
		Empty,
		PartitionDefinitionHash,
		PartitionDefinitionRange,
		PartitionDefinitionList,
	)
})

var PartitionDefinitionHash = NewFn(func(state *State) Fn {
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
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
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
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	return Or(
		SingleSelect,
		UnionSelect,
		MultiTableSelect,
	)
})

var SingleSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
	state.Store(ScopeKeyCurrentSelectedColNum, 1+rand.Intn(len(tbl.Columns)))
	return Or(
		CommonSelect,
		AggregationSelect,
		WindowSelect,
	)
})

var UnionSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl1, tbl2 := state.GetRandTable(), state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, Tables{tbl1, tbl2})
	colLen := mathutil.Min(len(tbl1.Columns), len(tbl2.Columns))
	state.Store(ScopeKeyCurrentSelectedColNum, 1+rand.Intn(colLen))
	return Or(
		And(Str("("), CommonSelect, Str(")"), SetOperator, Str("("), CommonSelect, Str(")")),
		And(
			Str("("), AggregationSelect, Str(")"), SetOperator, Str("("), AggregationSelect, Str(")"),
			Str("order by aggCol"), Opt(Strs("limit", RandomNum(1, 1000))),
		),
		And(
			Str("("), WindowSelect, Str(")"), SetOperator, Str("("), WindowSelect, Str(")"),
			Str("order by 1"), Opt(Strs("limit", RandomNum(1, 1000))),
		),
	)
})

var CommonSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables),
		MustHaveKey(ScopeKeyCurrentSelectedColNum)) {
		return None
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	cols := tbl.Columns.GetRandNColumns(state.Search(ScopeKeyCurrentSelectedColNum).ToInt())
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
	orderByCols := tbl.Columns.GetRandColumnsNonEmpty()
	state.Store(ScopeKeyCurrentOrderByColumns, NewTableColumnPairs1ToN(tbl, orderByCols))
	if state.Exists(ScopeKeyCurrentPrepare) {
		paramCols := SwapOutParameterizedColumns(cols)
		prepare := state.Search(ScopeKeyCurrentPrepare).ToPrepare()
		prepare.AppendColumns(paramCols...)
	}
	return And(Str("select"), HintTiFlash, HintIndexMerge,
		Str(PrintColumnNamesWithoutPar(cols, "*")),
		Str("from"), Str(tbl.Name), Str("where"),
		Predicates, Opt(OrderByLimit), ForUpdateOpt,
	)
})

var AggregationSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
	pkCols := tbl.GetUniqueKeyColumns()
	return And(
		Str("select"), HintAggToCop, AggFunction, Str("aggCol"),
		Str("from"),
		Str("(select"), HintTiFlash, HintIndexMerge, Str("*"),
		Str("from"), Str(tbl.Name), Str("where"), Predicates,
		If(len(pkCols) != 0, Strs("order by", PrintColumnNamesWithoutPar(pkCols, ""))),
		Str(") ordered_tbl"),
		GroupByColumnsOpt,
		Opt(Str("order by aggCol")),
		Opt(Strs("limit", RandomNum(1, 1000))),
		ForUpdateOpt,
	)
})

var GroupByColumnsOpt = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := tbl.Columns.GetRandColumnsNonEmpty()
	return Opt(Strs("group by", PrintColumnNamesWithoutPar(cols, "")))
})

var AggFunction = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	c1, c2 := Str(tbl.GetRandColumn().Name), Str(tbl.GetRandColumn().Name)
	distinctOpt := Opt(Str("distinct"))
	return Or(
		Strf("count([%fn] [%fn])", distinctOpt, c1),
		Strf("sum([%fn] [%fn])", distinctOpt, c1),
		Strf("avg([%fn] [%fn])", distinctOpt, c1),
		Strf("max([%fn] [%fn])", distinctOpt, c1),
		Strf("min([%fn] [%fn])", distinctOpt, c1),
		Strf("group_concat([%fn] [%fn] order by [%fn])", distinctOpt, c1, c1),
		Strf("bit_or([%fn])", c1),
		Strf("bit_xor([%fn])", c1),
		Strf("bit_and([%fn])", c1),
		Strf("var_pop([%fn] [%fn])", distinctOpt, c1),
		Strf("var_samp([%fn] [%fn])", distinctOpt, c1),
		Strf("stddev_pop([%fn] [%fn])", distinctOpt, c1),
		Strf("stddev_samp([%fn] [%fn])", distinctOpt, c1),
		Strf("json_objectagg([%fn], [%fn])", c1, c2),
		Strf("approx_count_distinct([%fn])", c1),
		Strf("approx_percentile([%fn], [%fn])", c1, Str(RandomNum(0, 100))),
	)
})

var WindowSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
	return And(
		Str("select"), HintTiFlash, HintIndexMerge, WindowFunction, Str("over w"),
		Str("from"), Str(tbl.Name), Str("window w as"), WindowClause,
		Opt(And(
			Str("order by"),
			Str(PrintColumnNamesWithoutPar(tbl.Columns, "")),
			Str(", "),
			WindowFunction,
			Str("over w"),
		)),
		Opt(Strs("limit", RandomNum(1, 1000))),
		ForUpdateOpt,
	)
})

var WindowClause = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	return And(Str("("), Opt(WindowPartitionBy), WindowOrderBy, Opt(WindowFrame), Str(")"))
})

var WindowPartitionBy = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := tbl.Columns.GetRandColumnsNonEmpty()
	return Strs("partition by", PrintColumnNamesWithoutPar(cols, ""))
})

var WindowOrderBy = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := tbl.Columns.GetRandColumnsNonEmpty()
	return Strs("order by", PrintColumnNamesWithoutPar(cols, ""))
})

var WindowFrame = NewFn(func(state *State) Fn {
	frames := []string{
		fmt.Sprintf("%d preceding", rand.Intn(5)),
		"current row",
		fmt.Sprintf("%d following", rand.Intn(5)),
	}
	get := func(idx int) interface{} { return frames[idx] }
	set := func(idx int, v interface{}) { frames[idx] = v.(string) }
	Move(rand.Intn(len(frames)), 0, get, set)
	return Strs("rows between", frames[1], "and", frames[2])
})

var WindowFunction = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	col := Str(tbl.GetRandColumn().Name)
	num := Str(RandomNum(1, 6))
	return Or(
		Str("row_number()"),
		Str("rank()"),
		Str("dense_rank()"),
		Str("cume_dist()"),
		Str("percent_rank()"),
		Strf("ntile([%fn])", num),
		Strf("lead([%fn],[%fn],NULL)", col, num),
		Strf("lag([%fn],[%fn],NULL)", col, num),
		Strf("first_value([%fn])", col),
		Strf("last_value([%fn])", col),
		Strf("nth_value([%fn],[%fn])", col, num),
	)
})

var ForUpdateOpt = NewFn(func(state *State) Fn {
	return Opt(Str("for update"))
})

var HintTiFlash = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	if !state.ExistsConfig(ConfigKeyUnitTiFlashQueryHint) {
		return Empty
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	return Strs("/*+ read_from_storage(tiflash[", PrintTableNames(tbls), "]) */")
})

var HintIndexMerge = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	return If(state.ExistsConfig(ConfigKeyUnitIndexMergeHint),
		Strs("/*+ use_index_merge(", PrintTableNames(tbls), ") */"),
	)
})

var HintAggToCop = NewFn(func(state *State) Fn {
	return And(
		Str("/*+"),
		Opt(Str("agg_to_cop()")),
		Or(Empty, Str("hash_agg()"), Str("stream_agg()")),
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
	lobCfg := state.SearchConfig(ConfigKeyEnumLimitOrderBy).ToStringOrDefault(ConfigKeyEnumLOBNone)
	if lobCfg == ConfigKeyEnumLOBNone {
		return Empty
	}
	tblColPairs := state.Search(ScopeKeyCurrentOrderByColumns).ToTableColumnPairs()
	switch lobCfg {
	case ConfigKeyEnumLOBOrderBy:
		return Strs("order by", PrintQualifiedColumnNames(tblColPairs))
	case ConfigKeyEnumLOBLimitOrderBy:
		return Strs("order by", PrintQualifiedColumnNames(tblColPairs), "limit", RandomNum(1, 1000))
	default:
		NeverReach()
		return Empty
	}
})

var CommonInsertOrReplace = NewFn(func(state *State) Fn {
	tbl := state.GetRandTable()
	state.Store(ScopeKeyCurrentTables, Tables{tbl})
	var cols []*Column
	if state.ExistsConfig(ConfigKeyUnitNonStrictTransTable) {
		cols = tbl.Columns.GetRandColumnsNonEmpty()
	} else {
		cols = tbl.GetRandColumnsIncludedDefaultValue()
	}
	state.Store(ScopeKeyCurrentSelectedColumns, cols)
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
	if len(cols) == 0 {
		cols = tbl.Columns
	}
	return And(
		Str("insert"), Opt(Str("ignore")), Str("into"), Str(tbl.Name),
		Str("set"),
		Str(PrintRandomAssignments(cols)),
		Opt(OnDuplicateUpdate),
	)
})

var CommonInsertValues = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	return And(
		Str("insert"), Opt(Str("ignore")), Str("into"), Str(tbl.Name),
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
	if len(cols) == 0 {
		cols = tbl.Columns
	}
	return And(
		Str("replace into"), Str(tbl.Name),
		Str("set"),
		Str(PrintRandomAssignments(cols)),
	)
})

var MultipleRowVals = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := state.Search(ScopeKeyCurrentSelectedColumns).ToColumns()
	var rowVal = NewFn(func(state *State) Fn {
		vs := tbl.GenRandValues(cols)
		return Strs("(", PrintRandValues(vs), ")")
	})
	return Repeat(rowVal.SetR(1, 7), Str(","))
})

var AssignClause = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	anotherTbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	col := tbl.GetRandColumn()
	return And(
		Strs(tbl.Name, ".", col.Name, "="),
		Or(
			Str(col.RandomValue()),
			Strs(anotherTbl.Name, ".", anotherTbl.GetRandColumn().Name),
		),
	)
})

var OnDuplicateUpdate = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	cols := tbl.Columns.GetRandColumnsNonEmpty()
	return Strs(
		"on duplicate key update",
		PrintRandomAssignments(cols),
	)
})

var CommonUpdate = NewFn(func(state *State) Fn {
	tbls := state.GetRandTableOrCTEs()
	state.Store(ScopeKeyCurrentTables, tbls)
	tbl := tbls[rand.Intn(len(tbls))]
	state.Store(ScopeKeyCurrentOrderByColumns, NewTableColumnPairs1ToN(tbl, tbl.Columns.GetRandColumnsNonEmpty()))
	return And(
		Str("update"),
		Str(PrintTableNames(tbls)),
		Str("set"),
		Repeat(AssignClause.SetR(1, 3), Str(",")),
		Str("where"),
		Predicates,
		Opt(OrderByLimit),
	)
})

var AnalyzeTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	return And(Str("analyze table"), Str(tbl.Name))
})

var CommonDelete = NewFn(func(state *State) Fn {
	tbls := state.GetRandTableOrCTEs()
	state.Store(ScopeKeyCurrentTables, tbls)
	whereTbl := tbls[rand.Intn(len(tbls))]
	var whereCol *Column
	if state.ExistsConfig(ConfigKeyUnitFirstColumnIndexable) {
		whereCol = whereTbl.GetRandIndexFirstColumn()
	} else {
		whereCol = whereTbl.GetRandColumn()
	}
	orderByTbl := tbls[rand.Intn(len(tbls))]
	state.Store(ScopeKeyCurrentOrderByColumns, NewTableColumnPairs1ToN(orderByTbl, orderByTbl.Columns.GetRandColumnsNonEmpty()))

	var randRowVal = NewFn(func(state *State) Fn {
		return Str(whereCol.RandomValue())
	})

	return And(
		Str("delete"),
		Str(PrintTableNames(tbls)),
		Str("from"),
		Str(PrintTableNames(tbls)),
		Str("where"),
		Or(
			And(Predicates),
			And(
				Str(PrintQualifiedColumnName(whereTbl, whereCol)),
				Str("in"),
				Str("("),
				Repeat(randRowVal.SetR(1, 9), Str(",")),
				Str(")")),
			And(Str(whereCol.Name), Str("is null")),
		),
		Opt(OrderByLimit),
	)
})

var Predicates = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	uniqueIdx := tbl.GetRandUniqueIndexForPointGet()
	if uniqueIdx != nil {
		state.Store(ScopeKeyCurrentUniqueIndexForPointGet, uniqueIdx)
	}
	return Or(
		Repeat(Predicate.SetR(1, 5), AndOr).SetW(3),
		PredicatesPointGet.SetW(1),
		PredicatesIndexMerge.SetW(4),
	)
})

var PredicatesIndexMerge = NewFn(func(state *State) Fn {
	if !state.ExistsConfig(ConfigKeyUnitIndexMergePredicate) {
		return None
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	tbl.colForPrefixIndex = tbl.GetRandIndexPrefixColumn()
	repeatCnt := len(tbl.colForPrefixIndex)
	if repeatCnt == 0 {
		repeatCnt = 1
	}
	if rand.Intn(5) == 0 {
		repeatCnt += rand.Intn(2) + 1
	}
	return RepeatCount(Predicate, repeatCnt, Str("and"))
})

var PredicatesPointGet = NewFn(func(state *State) Fn {
	if !state.Exists(ScopeKeyCurrentUniqueIndexForPointGet) {
		return None
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
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().PickOne()
	randCol := tbl.GetRandColumn()
	if state.ExistsConfig(ConfigKeyUnitIndexMergePredicate) {
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
		return Repeat(randVal.SetR(1, 5), Str(","))
	})
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	pre := Or(
		And(Str(colName), CompareSymbol, randVal),
		And(Str(colName), Str("in"), Str("("), randColVals, Str(")")),
		And(Str("IsNull("), Str(colName), Str(")")),
		And(Str(colName), Str("between"), randVal, Str("and"), randVal),
		And(Str(colName), Str("not in"), Str("("), randColVals, Str(")")),
	)
	return Or(
		pre,
		And(Str("not("), pre, Str(")")),
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

var AddIndex = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	idx := state.GenNewIndex(tbl)
	if idx == nil {
		// TODO: add warning log.
		return Empty
	}
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
		return None
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	idx := tbl.GetRandomIndex()
	if idx.Tp == IndexTypePrimary && state.ExistsConfig(ConfigKeyUnitAvoidDropPrimaryKey) {
		// we are not support drop primary when it's clustered.
		return None
	}
	tbl.RemoveIndex(idx)
	if idx.Tp == IndexTypePrimary {
		tbl.containsPK = false
		return Strs("alter table", tbl.Name, "drop primary key")
	}
	return Strs(
		"alter table", tbl.Name,
		"drop index", idx.Name,
	)
})

var AddColumn = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	tps := state.SearchConfig(ConfigKeyArrayAllowColumnTypes).
		ToConfigAllowedColumnTypes().AddColumnOrDefault()
	col := state.GenNewColumnWithType(tps...)
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
		return None
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
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := tbl.GetRandColumn()
	if state.ExistsConfig(ConfigKeyUnitAvoidAlterPKColumn) {
		col = tbl.GetRandNonPKColumn()
		if col == nil {
			return None
		}
	}
	tps := state.SearchConfig(ConfigKeyArrayAllowColumnTypes).
		ToConfigAllowedColumnTypes().ModifyColumnOrDefault()
	newCol := state.GenNewColumnWithType(tps...)
	tbl.ReplaceColumn(col, newCol)
	state.Store(ScopeKeyCurrentModifyColumn, newCol)
	if RandomBool() {
		newCol.Name = col.Name
		return And(Str("alter table"), Str(tbl.Name), Str("modify column"),
			Str(col.Name), Str(PrintColumnType(newCol)), ColumnPositionOpt)
	}
	return And(Str("alter table"), Str(tbl.Name), Str("change column"),
		Str(col.Name), Str(newCol.Name), Str(PrintColumnType(newCol)), ColumnPositionOpt)
})

var ColumnPositionOpt = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(
		MustHaveKey(ScopeKeyCurrentTables),
		MustHaveKey(ScopeKeyCurrentModifyColumn)) {
		return None
	}
	return Or(
		Empty,
		ColumnPositionFirst,
		ColumnPositionAfter,
	)
})

var ColumnPositionFirst = NewFn(func(state *State) Fn {
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := state.Search(ScopeKeyCurrentModifyColumn).ToColumn()
	tbl.MoveColumnToFirst(col)
	return Str("first")
})

var ColumnPositionAfter = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MoreThan1Columns) {
		return None
	}
	tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
	col := state.Search(ScopeKeyCurrentModifyColumn).ToColumn()
	restCols := tbl.FilterColumns(func(c *Column) bool {
		return c.ID != col.ID
	})
	afterCol := restCols[rand.Intn(len(restCols))]
	tbl.MoveColumnAfterColumn(col, afterCol)
	return Strs("after", afterCol.Name)
})

var MultiTableSelect = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasAtLeast2Tables) {
		return None
	}
	tables := state.tables.PickN(2)
	tbl1, tbl2 := tables[0], tables[1]
	cols1 := tbl1.GetRandColumns()
	cols2 := tbl2.GetRandColumns()
	state.Store(ScopeKeyCurrentTables, Tables{tbl1, tbl2})
	if RandomBool() {
		state.Store(ScopeKeyJoinPreferIndex, struct{}{})
	}

	orderByCols := ConcatColumnPairs(tbl1, tbl2, tbl1.Columns, tbl2.Columns)
	state.Store(ScopeKeyCurrentOrderByColumns, orderByCols)
	return Or(
		And(
			Str("select"), HintTiFlash, HintJoin,
			Str(PrintFullQualifiedColName(tbl1, cols1)),
			Str(","),
			Str(PrintFullQualifiedColName(tbl2, cols2)),
			Str("from"),
			Str(tbl1.Name),
			Or(Str("left join"), Str("join"), Str("right join")),
			Str(tbl2.Name),
			And(Str("on"), Repeat(JoinPredicate.SetR(1, 5), AndOr)),
			And(Str("where")),
			Predicates,
			Opt(OrderByLimit),
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
			Opt(OrderByLimit),
		),
		SemiJoinStmt,
	)
})

var SemiJoinStmt = NewFn(func(state *State) Fn {
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	t1, t2 := tbls[0], tbls[1]
	var cols1, cols2 []*Column
	if state.Exists(ScopeKeyJoinPreferIndex) {
		cols1 = t1.FilterColumns(func(c *Column) bool {
			return c.ColumnHasIndex(t1)
		})
		cols2 = t2.FilterColumns(func(c *Column) bool {
			return c.ColumnHasIndex(t2)
		})
	}
	if len(cols1) == 0 {
		cols1 = t1.Columns
	}
	if len(cols2) == 0 {
		cols2 = t2.Columns
	}
	c1, c2 := RandomCompatibleColumnPair(cols1, cols2)
	state.Store(ScopeKeyCurrentOrderByColumns, NewTableColumnPairs1ToN(t1, t1.Columns))
	// TODO: Support exists subquery.
	return And(
		Str("select"), HintTiFlash, HintJoin,
		Str(PrintFullQualifiedColName(t1, cols1)),
		Str("from"), Str(t1.Name),
		Str("where"), Str(c1.Name), Str("in"), Str("("),
		Str("select"), Str(c2.Name), Str("from"), Str(t2.Name),
		Str("where"), Predicates,
		Str(")"),
		Opt(OrderByLimit),
	)
})

var JoinPredicate = NewFn(func(state *State) Fn {
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	t1, t2 := tbls[0], tbls[1]
	var col1, col2 *Column
	if state.Exists(ScopeKeyJoinPreferIndex) {
		col1 = t1.GetRandColumnsPreferIndex()
		col2 = t2.GetRandColumnsPreferIndex()
	} else {
		col1 = t1.GetRandColumn()
		col2 = t2.GetRandColumn()
	}
	return And(
		Str(PrintQualifiedColumnName(t1, col1)),
		CompareSymbol,
		Str(PrintQualifiedColumnName(t2, col2)),
	)
})

var HintJoin = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(MustHaveKey(ScopeKeyCurrentTables)) {
		return None
	}
	return Or(
		Empty,
		JoinHintWithoutIndex,
		JoinHintWithIndex,
	)
})

var JoinHintWithoutIndex = NewFn(func(state *State) Fn {
	if state.Exists(ScopeKeyJoinPreferIndex) {
		return None
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	t1, t2 := tbls[0], tbls[1]
	return Or(
		Strs("/*+ merge_join(", t1.Name, ",", t2.Name, "*/"),
		Strs("/*+ hash_join(", t1.Name, ",", t2.Name, "*/"),
	)
})

var JoinHintWithIndex = NewFn(func(state *State) Fn {
	if !state.Exists(ScopeKeyJoinPreferIndex) {
		return None
	}
	tbls := state.Search(ScopeKeyCurrentTables).ToTables()
	t1, t2 := tbls[0], tbls[1]
	return Or(
		Strs("/*+ inl_join(", t1.Name, ",", t2.Name, ") */"),
		Strs("/*+ inl_hash_join(", t1.Name, ",", t2.Name, ") */"),
		Strs("/*+ inl_merge_join(", t1.Name, ",", t2.Name, ") */"),
	)
})

var AndOr = NewFn(func(state *State) Fn {
	return Or(
		Str("and"),
		Str("or"),
	)
})

var CreateTableLike = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(NoTooMuchTables, HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	newTbl := tbl.CloneCreateTableLike(state)
	state.AppendTable(newTbl)
	return Strs("create table", newTbl.Name, "like", tbl.Name)
})

var SelectIntoOutFile = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
	}
	tbl := state.GetRandTable()
	state.StoreInRoot(ScopeKeyLastOutFileTable, tbl)
	_ = os.RemoveAll(SelectOutFileDir)
	_ = os.Mkdir(SelectOutFileDir, 0755)
	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, state.AllocGlobalID(ScopeKeyTmpFileID)))
	return Strs("select * from", tbl.Name, "into outfile", fmt.Sprintf("'%s'", tmpFile))
})

var LoadTable = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, AlreadySelectOutfile) {
		return None
	}
	tbl := state.Search(ScopeKeyLastOutFileTable).ToTable()
	id := state.Search(ScopeKeyTmpFileID).ToInt()
	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, id))
	randChildTable := tbl.childTables[rand.Intn(len(tbl.childTables))]
	return Strs("load data local infile", fmt.Sprintf("'%s'", tmpFile), "into table", randChildTable.Name)
})

var SplitRegion = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables) {
		return None
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
		return None
	}
	prepare := GenNewPrepare(state.AllocGlobalID(ScopeKeyPrepareID))
	state.AppendPrepare(prepare)
	state.Store(ScopeKeyCurrentPrepare, prepare)
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
		return None
	}
	prepare := state.GetRandPrepare()
	state.RemovePrepare(prepare)
	return Strs("deallocate prepare", prepare.Name)
})

var QueryPrepare = NewFn(func(state *State) Fn {
	if !state.CheckAssumptions(HasTables, HasPreparedStmts) {
		return None
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

var UnionOption = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		Str("DISTINCT"),
		Str("ALL"),
	)
})

// a replacement to avoid initialization loop
var CTEQueryStatementReplacement Fn

var CTEQueryStatement = NewFn(func(state *State) Fn {
	return And(WithClause, SimpleCTEQuery)
})

var CTEDMLStatement = NewFn(func(state *State) Fn {
	state.ctes = state.ctes[:0]
	return And(
		WithClause,
		DMLStmt,
	)
})

var SimpleCTEQuery = NewFn(func(state *State) Fn {
	parentCTE := state.ParentCTE()
	ctes := state.PopCTE()
	if rand.Intn(10) == 0 {
		c := rand.Intn(len(ctes))
		for i := 0; i < c; i++ {
			ctes = append(ctes, ctes[rand.Intn(len(ctes))])
		}
	}

	rand.Shuffle(len(ctes), func(i, j int) {
		ctes[j], ctes[i] = ctes[i], ctes[j]
	})

	//ctes = ctes[:rand.Intn(mathutil.Min(len(ctes), 2))+1]

	cteNames := make([]string, 0, len(ctes))
	colsInfo := make(map[ColumnType][]string)
	colNames := make([]string, 0)
	colNames = append(colNames, "1")
	for i := range ctes {
		ctes[i].AsName = fmt.Sprintf("cte_as_%d", state.AllocGlobalID(ScopeKeyCTEAsNameID))
		cteNames = append(cteNames, fmt.Sprintf("%s as %s", ctes[i].Name, ctes[i].AsName))
		for _, col := range ctes[i].Columns {
			if _, ok := colsInfo[col.Tp]; !ok {
				colsInfo[col.Tp] = make([]string, 0)
			}
			colsInfo[col.Tp] = append(colsInfo[col.Tp], fmt.Sprintf("%s.%s", ctes[i].AsName, col.Name))
			colNames = append(colNames, fmt.Sprintf("%s.%s", ctes[i].AsName, col.Name))
		}
	}

	// todo: it can infer the cte or the common table
	if parentCTE != nil {
		colNames = colNames[:1]
		for _, c := range parentCTE.Columns[1:] {
			if _, ok := colsInfo[c.Tp]; ok {
				colNames = append(colNames, colsInfo[c.Tp][rand.Intn(len(colsInfo[c.Tp]))])
			} else {
				colNames = append(colNames, PrintConstantWithFunction(c.Tp))
			}
		}
	}
	orderByFields := make([]string, len(colNames))
	for i := range orderByFields {
		orderByFields[i] = fmt.Sprintf("%d", i+1)
	}
	return And(
		Str("("),
		Str("select"),
		Str(strings.Join(colNames, ",")),
		Str("from"),
		Str(strings.Join(cteNames, ",")),
		If(rand.Intn(10) == 0,
			And(
				Str("where exists ("),
				Query,
				Str(")"),
			),
		),
		If(parentCTE == nil,
			And(
				Str("order by"),
				Str(strings.Join(orderByFields, ",")),
			),
		),
		And(Str("limit"), Str(RandomNum(0, 20))),
		Str(")"),
	)
})

var WithClause = NewFn(func(state *State) Fn {
	validSQLPercent := state.SearchConfig(ConfigKeyCTEValidSQLPercent).ToIntOrDefault(75)
	state.IncCTEDeep()
	return And(
		Str("with"),
		Or(
			If(ShouldValid(validSQLPercent), Str("recursive")),
			Str("recursive"),
		),
		Repeat(CTEDefinition.SetR(1, 3), Str(",")),
	)
})

var CTEDefinition = NewFn(func(state *State) Fn {
	validSQLPercent := state.SearchConfig(ConfigKeyCTEValidSQLPercent).ToIntOrDefault(75)
	cte := state.GenNewCTE()
	colCnt := state.ParentCTEColCount()
	if colCnt == 0 {
		colCnt = 2
	}
	cte.AppendColumn(state.GenNewColumnWithType(ColumnTypeInt))
	for i := 0; i < colCnt+rand.Intn(2); i++ {
		cte.AppendColumn(state.GenNewColumnWithType(ColumnTypeInt, ColumnTypeChar))
	}
	if !ShouldValid(validSQLPercent) {
		if RandomBool() && state.GetCTECount() != 0 {
			cte.Name = state.GetRandomCTE().Name
		} else {
			cte.Name = state.GetRandTable().Name
		}
	}
	state.PushCTE(cte)

	return And(
		Str(cte.Name),
		Strs("(", PrintColumnNamesWithoutPar(cte.Columns, ""), ")"),
		Str("AS"),
		CTEExpressionParens,
	)
})

var CTESeedPart = NewFn(func(state *State) Fn {
	validSQLPercent := state.SearchConfig(ConfigKeyCTEValidSQLPercent).ToIntOrDefault(75)
	tbl := state.GetRandTable()
	currentCTE := state.CurrentCTE()
	fields := make([]string, len(currentCTE.Columns)-1)
	for i := range fields {
		switch rand.Intn(4) {
		case 0, 3:
			cols := tbl.FilterColumns(func(column *Column) bool {
				return column.Tp == currentCTE.Columns[i+1].Tp
			})
			if len(cols) != 0 {
				fields[i] = cols[rand.Intn(len(cols))].Name
				continue
			}
			fallthrough
		case 1:
			fields[i] = currentCTE.Columns[i+1].RandomValue()
		case 2:
			if ShouldValid(validSQLPercent) {
				fields[i] = PrintConstantWithFunction(currentCTE.Columns[i+1].Tp)
			} else {
				fields[i] = fmt.Sprintf("a") // for unknown column
			}
		}
	}

	if !ShouldValid(validSQLPercent) {
		fields = append(fields, "1")
	}

	return Or(
		And(
			Str("select 1,"),
			Str(strings.Join(fields, ",")),
			Str("from"),
			Str(tbl.Name), // todo: it can refer the exist cte and the common table
		).SetW(5),
		CTEQueryStatementReplacement,
	)
})

var CTERecursivePart = NewFn(func(state *State) Fn {
	validSQLPercent := state.SearchConfig(ConfigKeyCTEValidSQLPercent).ToIntOrDefault(75)
	lastCTE := state.CurrentCTE()
	if !ShouldValid(validSQLPercent) {
		lastCTE = state.GetRandomCTE()
	}
	fields := append(make([]string, 0, len(lastCTE.Columns)), fmt.Sprintf("%s + 1", lastCTE.Columns[0].Name))
	for _, col := range lastCTE.Columns[1:] {
		fields = append(fields, PrintColumnWithFunction(col))
	}
	if !ShouldValid(validSQLPercent) {
		rand.Shuffle(len(fields[1:]), func(i, j int) {
			fields[1+i], fields[1+j] = fields[1+j], fields[1+i]
		})
		if rand.Intn(20) == 0 {
			fields = append(fields, "1")
		}
	}

	// todo: recursive part can be a function, const
	return Or(
		And(
			Str("select"),
			Str(strings.Join(fields, ",")),
			Str("from"),
			Str(lastCTE.Name), // todo: it also can be a cte
			Str("where"),
			Str(fmt.Sprintf("%s < %d", lastCTE.Columns[0].Name, 5)),
			Opt(And(Str("limit"), Str(RandomNum(0, 20)))),
		),
	)
})

var CTEExpressionParens = NewFn(func(state *State) Fn {
	return And(
		Str("("),
		CTESeedPart,
		Opt(
			And(
				Str("UNION"),
				UnionOption,
				CTERecursivePart,
			),
		),
		Str(")"))
})

func init() {
	CTEQueryStatementReplacement = CTEQueryStatement
}
