package sqlgen

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cznic/mathutil"
)

var Query = NewFn(func(state *State) Fn {
	return Or(
		SingleSelect,
		MultiSelect,
		UnionSelect,
	)
}).P(HasTables)

var QueryAll = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	orderByAllCols := PrintColumnNamesWithoutPar(tbl.Columns, "")
	return Strs("select * from", tbl.Name, "order by", orderByAllCols)
}).P(HasTables)

var UnionSelect = NewFn(func(state *State) Fn {
	tbl1, tbl2 := state.Tables.Rand(), state.Tables.Rand()
	fieldNum := mathutil.Min(len(tbl1.Columns), len(tbl2.Columns))
	state.env.Table = tbl1
	state.env.QState = &QueryState{FieldNumHint: fieldNum, SelectedCols: map[*Table]QueryStateColumns{
		tbl1: {
			Columns: tbl1.Columns,
			Attr:    make([]string, len(tbl1.Columns)),
		},
	}}
	firstSelect, err := CommonSelect.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	setOpr, err := SetOperator.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	state.env.Table = tbl2
	state.env.QState = &QueryState{FieldNumHint: fieldNum, SelectedCols: map[*Table]QueryStateColumns{
		tbl2: {
			Columns: tbl2.Columns,
			Attr:    make([]string, len(tbl2.Columns)),
		},
	}}
	secondSelect, err := CommonSelect.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	return Strs(
		"(", firstSelect, ")",
		setOpr,
		"(", secondSelect, ")",
		"order by 1 limit", RandomNum(1, 1000),
	)
})

var SingleSelect = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl: {
				Columns: tbl.Columns,
				Attr:    make([]string, len(tbl.Columns)),
			},
		},
	}
	return CommonSelect
})

var MultiSelect = NewFn(func(state *State) Fn {
	tbl1 := state.Tables.Rand()
	tbl2 := state.Tables.Rand()
	state.env.QState = &QueryState{
		SelectedCols: map[*Table]QueryStateColumns{
			tbl1: {
				Columns: tbl1.Columns,
				Attr:    make([]string, len(tbl1.Columns)),
			},
			tbl2: {
				Columns: tbl2.Columns,
				Attr:    make([]string, len(tbl2.Columns)),
			},
		},
	}
	return CommonSelect
})

var CommonSelect = NewFn(func(state *State) Fn {
	NotNil(state.env.QState)
	return And(
		Str("select"), HintTiFlash, Opt(HintIndexMerge), Opt(HintAggToCop), HintJoin,
		SelectFields, Str("from"), TableReference,
		WhereClause, GroupByColumnsOpt, WindowClause, Opt(OrderByLimit), ForUpdateOpt,
	)
})

var SelectFields = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if queryState.FieldNumHint == 0 {
		queryState.FieldNumHint = 1 + rand.Intn(5)
	}
	var fns []Fn
	for i := 0; i < queryState.FieldNumHint; i++ {
		fieldID := fmt.Sprintf("r%d", i)
		fns = append(fns, NewFn(func(state *State) Fn {
			state.env.Table = queryState.GetRandTable()
			state.env.QColumns = queryState.SelectedCols[state.env.Table]
			return And(SelectField, Str("as"), Str(fieldID))
		}))
		if i != queryState.FieldNumHint-1 {
			fns = append(fns, Str(","))
		}
	}
	return And(fns...)
})

var SelectField = NewFn(func(state *State) Fn {
	NotNil(state.env.Table)
	NotNil(state.env.QColumns)
	return Or(
		AggFunction,
		BuiltinFunction,
		SelectFieldName,
		WindowFunctionOverW,
	)
})

var SelectFieldName = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := state.env.QColumns
	c := cols.Rand()
	return Str(fmt.Sprintf("%s.%s", tbl.Name, c.Name))
})

var TableReference = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	tbNames := make([]Fn, 0, len(queryState.SelectedCols))
	for t := range queryState.SelectedCols {
		tbNames = append(tbNames, Str(t.Name))
	}
	if len(tbNames) == 1 {
		return tbNames[0]
	}
	return Or(
		Join(tbNames, Str(",")),
		And(Join(tbNames, JoinType), Str("on"), JoinPredicate),
	)
})

var JoinType = NewFn(func(state *State) Fn {
	return Or(
		Str("left join"),
		Str("right join"),
		Str("join"),
	)
})

var JoinPredicate = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var (
		preds     []string
		prevTable *Table
		prevCol   *Column
	)
	for t, cols := range queryState.SelectedCols {
		col := cols.Rand()
		if prevTable != nil {
			preds = append(preds,
				fmt.Sprintf("%s.%s = %s.%s",
					prevTable.Name, prevCol.Name,
					t.Name, col.Name))
		}
		prevTable = t
		prevCol = col
	}
	return Str(strings.Join(preds, " and "))
})

var GroupByColumnsOpt = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var groupByItems []string
	for t, scs := range queryState.SelectedCols {
		for i, c := range scs.Columns {
			if scs.Attr[i] == QueryAggregation {
				groupByItems = append(groupByItems, fmt.Sprintf("%s.%s", t.Name, c.Name))
			}
		}
	}
	if len(groupByItems) == 0 {
		return Empty
	}
	return Opt(Strs("group by", strings.Join(groupByItems, ",")))
})

var WhereClause = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		And(Str("where"), Or(Predicates, Predicate)).W(3),
	)
})

var HintJoin = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if len(queryState.SelectedCols) != 2 {
		return Empty
	}
	var tbl []*Table
	for t := range queryState.SelectedCols {
		tbl = append(tbl, t)
	}
	t1, t2 := tbl[0], tbl[1]
	return Or(
		Empty,
		Strs("/*+ merge_join(", t1.Name, ",", t2.Name, "*/"),
		Strs("/*+ hash_join(", t1.Name, ",", t2.Name, "*/"),
		Strs("/*+ inl_join(", t1.Name, ",", t2.Name, ") */"),
		Strs("/*+ inl_hash_join(", t1.Name, ",", t2.Name, ") */"),
		Strs("/*+ inl_merge_join(", t1.Name, ",", t2.Name, ") */"),
	)
})

var WindowClause = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	if !queryState.IsWindow {
		return Empty
	}
	for t := range queryState.SelectedCols {
		state.env.Table = t
	}
	return And(
		Str("window w as"),
		Str("("),
		Opt(WindowPartitionBy),
		WindowOrderBy,
		Opt(WindowFrame),
		Str(")"),
	)
})

var WindowPartitionBy = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := tbl.Columns.RandNNotNil()
	return Strs("partition by", PrintColumnNamesWithoutPar(cols, ""))
})

var WindowOrderBy = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := tbl.Columns.RandNNotNil()
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

var WindowFunctionOverW = NewFn(func(state *State) Fn {
	NotNil(state.env.QState)
	return And(WindowFunction, Str("over w"))
}).P(func(state *State) bool {
	queryState := state.env.QState
	return len(queryState.SelectedCols) == 1
})

var WindowFunction = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	queryState.IsWindow = true
	var tbl *Table
	for t := range queryState.SelectedCols {
		tbl = t
	}
	col := Str(fmt.Sprintf("%s.%s", tbl.Name, tbl.Columns.Rand().Name))
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

var Predicates = NewFn(func(state *State) Fn {
	var pred []string
	for i := 0; i < 1+rand.Intn(2); i++ {
		if i != 0 {
			andor, err := AndOr.Eval(state)
			if err != nil {
				return NoneBecauseOf(err)
			}
			pred = append(pred, andor)
		}
		if state.env.QState != nil {
			state.env.Table = state.env.QState.GetRandTable()
		} else if state.env.Table == nil {
			state.env.Table = state.Tables.Rand()
		}
		state.env.Column = state.env.Table.Columns.Rand()
		p, err := Predicate.Eval(state)
		if err != nil {
			return NoneBecauseOf(err)
		}
		pred = append(pred, p)
	}
	return Str(strings.Join(pred, " "))
})

var Predicates2 Fn

func init() {
	Predicates2 = Predicates
}

var Predicate = NewFn(func(state *State) Fn {
	if state.env.QState != nil {
		state.env.Table = state.env.QState.GetRandTable()
	} else if state.env.Table == nil {
		state.env.Table = state.Tables.Rand()
	}
	state.env.Column = state.env.Table.Columns.Rand()
	tbl := state.env.Table
	randCol := state.env.Column
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	pre := Or(
		Or(
			And(Str(colName), CompareSymbol, RandVal),
			And(Str(colName), Str("in"), Str("("), InValues, Str(")")),
			And(Str("IsNull("), Str(colName), Str(")")),
			And(Str(colName), Str("between"), RandVal, Str("and"), RandVal),
		),
		JSONPredicate,
	)
	return Or(
		pre.W(5),
		And(Str("not("), pre, Str(")")),
	)
})

var JSONPredicate = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	randCol := state.env.Column
	colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
	pre := Or(
		And(RandVal, Str("MEMBER OF"), Str("("), Str(colName), Str(")")),
		And(Str("JSON_CONTAINS("), Str(colName), Str(","), RandVal, Str(")")),
		And(Str("JSON_CONTAINS("), RandVal, Str(","), Str(colName), Str(")")),
		And(Str("JSON_OVERLAPS("), Str(colName), Str(","), RandVal, Str(")")),
		And(Str("JSON_OVERLAPS("), RandVal, Str(","), Str(colName), Str(")")),
		And(Str("IsNull("), Str("JSON_OVERLAPS("), RandVal, Str(","), Str(colName), Str(")"), Str(")")),
	)
	return Or(
		pre,
		And(Str("not("), pre, Str(")")),
	)
})

var InValues = NewFn(func(state *State) Fn {
	if len(state.Tables) <= 1 {
		return RandColVals
	}
	return Or(
		RandColVals,
		SubSelect,
	)
})

var RandColVals = NewFn(func(state *State) Fn {
	return Repeat(RandVal.R(1, 5), Str(","))
})

var RandVal = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	randCol := state.env.Column
	var v string
	if len(tbl.Values) == 0 || rand.Intn(3) == 0 {
		v = randCol.RandomValue()
	} else {
		v = tbl.GetRandRowVal(randCol)
	}
	return Str(v)
})

var SubSelect = NewFn(func(state *State) Fn {
	tbl := state.Env().Table
	availableTbls := state.Tables
	if state.Env().IsIn("CommonDelete") || state.Env().IsIn("CommonUpdate") {
		availableTbls = availableTbls.Filter(func(t *Table) bool {
			return t.ID != tbl.ID
		})
	}
	subTbl := availableTbls.Rand()
	subCol := subTbl.Columns.Rand()
	return And(
		Str("select"), Str(subCol.Name), Str("from"), Str(subTbl.Name),
		Str("where"), Predicates2,
	)
})

var SubSelectWithGivenTp = NewFn(func(state *State) Fn {
	randCol := state.env.Column
	subTbl, subCol := GetRandTableColumnWithTp(state.Tables, randCol.Tp)
	return And(
		Str("select"), Str(subCol.Name), Str("from"), Str(subTbl.Name),
		Str("where"), Predicate,
	)
}).P(HasSameColumnType)

var ForUpdateOpt = NewFn(func(state *State) Fn {
	return Opt(Str("for update"))
})

var HintTiFlash = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var tbs []string
	for t := range queryState.SelectedCols {
		if t.TiflashReplica > 0 {
			tbs = append(tbs, t.Name)
		}
	}
	if len(tbs) == 0 {
		return Empty
	}
	return Strs("/*+ read_from_storage(tiflash[", strings.Join(tbs, ","), "]) */")
})

var HintIndexMerge = NewFn(func(state *State) Fn {
	queryState := state.env.QState
	var tbs []string
	for t := range queryState.SelectedCols {
		tbs = append(tbs, t.Name)
	}
	return Strs("/*+ use_index_merge(", strings.Join(tbs, ","), ") */")
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
	queryState := state.env.QState
	var fields strings.Builder
	if queryState == nil {
		return Empty
	}
	for i := 0; i < queryState.FieldNumHint; i++ {
		if i != 0 {
			fields.WriteString(",")
		}
		fields.WriteString(fmt.Sprintf("r%d", i))
	}
	return And(
		Str("order by"), Str(fields.String()),
		Opt(Strs("limit", RandomNum(1, 100))),
	)
})
