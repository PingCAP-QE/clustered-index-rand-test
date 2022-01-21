package sqlgen

import (
	"fmt"
	"math/rand"
	"strings"
)

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
		ctes[i].AsName = fmt.Sprintf("cte_as_%d", state.alloc.AllocCTEID())
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
	validSQLPercent := 75
	state.IncCTEDeep()
	return And(
		Str("with"),
		Or(
			If(ShouldValid(validSQLPercent), Str("recursive")),
			Str("recursive"),
		),
		Repeat(CTEDefinition.R(1, 3), Str(",")),
	)
})

var CTEDefinition = NewFn(func(state *State) Fn {
	validSQLPercent := 75
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
	validSQLPercent := 75
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
		).W(5),
		CTEQueryStatementReplacement,
	)
})

var CTERecursivePart = NewFn(func(state *State) Fn {
	validSQLPercent := 75
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

var UnionOption = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		Str("DISTINCT"),
		Str("ALL"),
	)
})

func init() {
	CTEQueryStatementReplacement = CTEQueryStatement
}
