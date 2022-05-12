package sqlgen

import (
	"fmt"
	"math/rand"
	"strings"
)

var Start = NewFn(func(state *State) Fn {
	return Or(
		SetSystemVars.W(2),
		AdminCheck.W(1).P(HasTables),
		CreateTable.W(13).P(NoTooMuchTables),
		CreateTableLike.W(6).P(HasTables, NoTooMuchTables),
		Query.W(20).P(HasTables),
		//QueryPrepare.W(2).P(HasTables),
		DMLStmt.W(20).P(HasTables),
		AlterTable.W(5).P(HasTables),
		SplitRegion.W(1).P(HasTables),
		AnalyzeTable.W(0).P(HasTables),
		//PrepareStmt.W(2).P(HasTables),
		//DeallocPrepareStmt.W(1).P(HasTables),
		FlashBackTable.W(1).P(HasDroppedTables),
		//SelectIntoOutFile.W(1).P(HasTables),
		//LoadTable.W(1).P(HasTables),
		DropTable.W(1).P(HasTables),
		TruncateTable.W(1).P(HasTables),
		SetTiFlashReplica.W(0).P(HasTables),
	)
})

var DMLStmt = NewFn(func(state *State) Fn {
	return Or(
		CommonDelete.W(1),
		CommonInsertOrReplace.W(3),
		CommonUpdate.W(1),
	)
})

var AlterTable = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.Table = tbl
	return And(Str("alter table"), Str(tbl.Name),
		Or(
			AlterTableChangeSingle,
			AlterTableChangeMulti.W(0),
		))
})

var AlterTableChangeMulti = NewFn(func(state *State) Fn {
	state.Env().MultiObjs = NewMultiObjs()
	return Repeat(AlterTableChangeSingle.R(2, 5), Str(", "))
})

var AlterTableChangeSingle = NewFn(func(state *State) Fn {
	return Or(
		AddColumn,
		AddIndex,
		DropColumn,
		DropIndex,
		AlterColumn,
	)
})

var SetSystemVars = NewFn(func(state *State) Fn {
	return Or(
		SwitchRowFormatVer,
		SwitchClustered,
	)
})

var SwitchRowFormatVer = NewFn(func(state *State) Fn {
	return Strs("set @@global.tidb_row_format_version =", RandomNum(1, 2))
})

var SwitchClustered = NewFn(func(state *State) Fn {
	return Strs("set @@global.tidb_enable_clustered_index =", RandomNum(0, 1))
})

var DropTable = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.RemoveTable(tbl)
	return Strs("drop table", tbl.Name)
})

var TruncateTable = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.TruncateTable(tbl)
	return Strs("truncate table", tbl.Name)
})

var CreateTable = NewFn(func(state *State) Fn {
	tbl := state.GenNewTable()
	state.Tables = state.Tables.Append(tbl)
	state.env.Table = tbl
	// The eval order matters because the dependency is ColumnDefinitions <- PartitionDefinition <- IndexDefinitions.
	eColDefs, err := ColumnDefinitions.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	state.env.PartColumn = tbl.Columns.Filter(func(c *Column) bool { return c.Tp.IsPartitionType() }).Rand()
	ePartitionDef, err := PartitionDefinition.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	eTableOption, err := TableOptions.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	eIdxDefs, err := IndexDefinitions.Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	if len(strings.Trim(eIdxDefs, " ")) != 0 {
		return Strs("create table", tbl.Name, "(", eColDefs, ",", eIdxDefs, ")",
			eTableOption, ePartitionDef)
	}
	return Strs("create table", tbl.Name, "(", eColDefs, ")",
		eTableOption, ePartitionDef)
})

var TableOptions = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	return Strs("charset", tbl.Collate.CharsetName, "collate", tbl.Collate.CollationName)
})

var InsertInto = NewFn(func(state *State) Fn {
	tbl := state.env.Table
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

var CommonInsertOrReplace = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.Table = tbl
	if RandomBool() {
		cWithDef, cWithoutDef := tbl.Columns.Span(func(c *Column) bool {
			return c.defaultVal != ""
		})
		state.env.Columns = cWithoutDef.Concat(cWithDef.RandN())
	}
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
	tbl := state.env.Table
	cols := state.env.Columns
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
	tbl := state.env.Table
	cols := state.env.Columns
	return And(
		Str("insert"), Opt(Str("ignore")), Str("into"), Str(tbl.Name),
		Str(PrintColumnNamesWithPar(cols, "")),
		Str("values"),
		MultipleRowVals,
		Opt(OnDuplicateUpdate),
	)
})

var CommonReplaceValues = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := state.env.Columns
	var sb strings.Builder
	for i, c := range cols {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(c.String())
	}
	return And(
		Str("replace into"), Str(tbl.Name),
		Str(PrintColumnNamesWithPar(cols, "")),
		Str("values"),
		MultipleRowVals,
	)
})

var CommonReplaceSet = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := state.env.Columns
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
	tbl := state.env.Table
	cols := state.env.Columns
	var rowVal = NewFn(func(state *State) Fn {
		vs := tbl.GenRandValues(cols)
		return Strs("(", PrintRandValues(vs), ")")
	})
	return Repeat(rowVal.R(1, 7), Str(","))
})

var AssignClause = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	col := tbl.Columns.Rand()
	return Strs(fmt.Sprintf("%s.%s", tbl.Name, col.Name), "=", col.RandomValue())
})

var OnDuplicateUpdate = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := tbl.Columns.RandNNotNil()
	return Strs(
		"on duplicate key update",
		PrintRandomAssignments(cols),
	)
})

var CommonUpdate = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.Table = tbl
	return And(
		Str("update"),
		Str(tbl.Name),
		Str("set"),
		Repeat(AssignClause.R(1, 3), Str(",")),
		Str("where"),
		Predicates,
		Opt(OrderByLimit),
	)
})

var AnalyzeTable = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	return And(Str("analyze table"), Str(tbl.Name))
})

var CommonDelete = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	state.env.Table = tbl
	col := tbl.Columns.Rand()
	var randRowVal = NewFn(func(state *State) Fn {
		return Str(col.RandomValue())
	})
	return And(
		Str("delete"),
		Str("from"),
		Str(tbl.Name),
		Str("where"),
		Or(
			And(Predicates),
			And(
				Str(fmt.Sprintf("%s.%s", tbl.Name, col.Name)),
				Str("in"),
				Str("("),
				Repeat(randRowVal.R(1, 9), Str(",")),
				Str(")")),
			And(Str(col.Name), Str("is null")),
		),
		Opt(OrderByLimit),
	)
})

var AddIndex = NewFn(func(state *State) Fn {
	NotNil(state.env.Table)
	return And(Str("add"), IndexDefinition)
})

var DropIndex = NewFn(func(state *State) Fn {
	tbl := state.Env().Table
	idxes := tbl.Indexes.Filter(func(index *Index) bool {
		// Not support operate the same object in multi-schema change.
		return !state.Env().MultiObjs.SameObject(index.Name)
	})
	if tbl.Clustered {
		// Cannot drop the clustered primary key.
		idxes = idxes.Filter(func(index *Index) bool {
			return index.Tp != IndexTypePrimary
		})
	}
	if len(idxes) == 0 {
		return None("no indexes can be dropped")
	}
	idx := idxes.Rand()
	tbl.RemoveIndex(idx)
	if idx.Tp == IndexTypePrimary {
		return Str("drop primary key")
	}
	return Strs("drop index", idx.Name)
})

var AddColumn = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	newCol := &Column{ID: state.alloc.AllocColumnID()}
	state.env.Column = newCol
	ret, err := And(
		Str("add column"),
		ColumnDefinitionName,
		ColumnDefinitionTypeOnAdd,
		ColumnDefinitionCollation,
		ColumnDefinitionUnsigned,
		ColumnDefinitionNotNull,
		ColumnDefinitionDefault,
	).Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	tbl.AppendColumn(newCol)
	if state.env.MultiObjs != nil {
		state.env.MultiObjs.AddName(newCol.Name)
	}
	return Str(ret)
})

var DropColumn = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	if len(tbl.Columns) < 2 {
		return None("columns less than 2")
	}
	multiColIdxes := tbl.Indexes.Filter(func(index *Index) bool {
		return len(index.Columns) > 1
	})
	pk := tbl.Indexes.Primary()
	cols := tbl.Columns.Filter(func(c *Column) bool {
		// Not support operate the same object in multi-schema change.
		if state.env.MultiObjs.SameObject(c.Name) {
			return false
		}
		// Not support drop with composite index covered.
		if multiColIdxes.Found(func(index *Index) bool {
			return index.HasColumn(c)
		}) {
			return false
		}
		// Not support drop with primary key covered.
		if pk != nil && pk.HasColumn(c) {
			return false
		}
		return true
	})
	if len(cols) == 0 {
		return None("no column can be dropped")
	}
	col := cols.Rand()
	tbl.RemoveColumn(col)
	return Strs("drop column", col.Name)
})

var AlterColumn = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	cols := tbl.Columns.Filter(func(c *Column) bool {
		// Not support operate the same object in multi-schema change.
		return !state.env.MultiObjs.SameObject(c.Name)
	})
	pk := tbl.Indexes.Primary()
	if pk != nil {
		// Not support modify/change primary key columns.
		cols = cols.Diff(pk.Columns)
	}
	if len(cols) == 0 {
		return None("no columns can be modified")
	}
	state.env.Column = cols.Rand()
	return Or(
		AlterColumnChange,
		AlterColumnModify,
	)
})

var AlterColumnChange = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	col := state.env.Column
	newCol := &Column{ID: state.alloc.AllocColumnID()}
	state.env.Column = newCol
	state.env.OldColumn = col
	ret, err := And(
		Str("change column"),
		Str(col.Name),
		ColumnDefinitionName,
		ColumnDefinitionTypeOnModify,
		ColumnDefinitionCollation,
		ColumnDefinitionUnsigned,
		ColumnDefinitionNotNull,
		ColumnDefinitionDefault,
	).Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	tbl.ModifyColumn(col, newCol)
	if state.env.MultiObjs != nil {
		state.env.MultiObjs.AddName(newCol.Name)
		state.env.MultiObjs.AddName(col.Name)
	}
	return And(Str(ret), ColumnPositionOpt)
})

var AlterColumnModify = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	col := state.env.Column
	newCol := &Column{ID: col.ID, Name: col.Name}
	state.env.Column = newCol
	state.env.OldColumn = col
	ret, err := And(
		Str("modify column"),
		Str(col.Name),
		ColumnDefinitionTypeOnModify,
		ColumnDefinitionCollation,
		ColumnDefinitionUnsigned,
		ColumnDefinitionNotNull,
		ColumnDefinitionDefault,
	).Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	tbl.ModifyColumn(col, newCol)
	if state.env.MultiObjs != nil {
		state.env.MultiObjs.AddName(col.Name)
	}
	return And(Str(ret), ColumnPositionOpt)
})

var ColumnPositionOpt = NewFn(func(state *State) Fn {
	return Or(
		Empty,
		ColumnPositionFirst,
		ColumnPositionAfter,
	)
})

var ColumnPositionFirst = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	col := state.env.Column
	tbl.MoveColumnToFirst(col)
	return Str("first")
})

var ColumnPositionAfter = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	if len(tbl.Columns) < 2 {
		return None("ColumnPositionAfter should have at lease 2 columns")
	}
	col := state.env.Column
	restCols := tbl.Columns.Filter(func(c *Column) bool {
		return c.ID != col.ID && !state.env.MultiObjs.SameObject(c.Name)
	})
	if len(restCols) == 0 {
		return None("cannot find after column")
	}
	afterCol := restCols[rand.Intn(len(restCols))]
	tbl.MoveColumnAfterColumn(col, afterCol)
	if state.env.MultiObjs != nil {
		state.env.MultiObjs.AddName(afterCol.Name)
	}
	return Strs("after", afterCol.Name)
})

var AndOr = NewFn(func(state *State) Fn {
	return Or(
		Str("and"),
		Str("or"),
	)
})

var CreateTableLike = NewFn(func(state *State) Fn {
	tbl := state.Tables.Rand()
	newTbl := tbl.CloneCreateTableLike(state)
	state.Tables = state.Tables.Append(newTbl)
	return Strs("create table", newTbl.Name, "like", tbl.Name)
})

//var SelectIntoOutFile = NewFn(func(state *State) Fn {
//	tbl := state.Tables.Rand()
//	state.StoreInRoot(ScopeKeyLastOutFileTable, tbl)
//	_ = os.RemoveAll(SelectOutFileDir)
//	_ = os.Mkdir(SelectOutFileDir, 0755)
//	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, state.AllocGlobalID(ScopeKeyTmpFileID)))
//	return Strs("select * from", tbl.Name, "into outfile", fmt.Sprintf("'%s'", tmpFile))
//})
//
//var LoadTable = NewFn(func(state *State) Fn {
//	tbl := state.env.Get(ScopeKeyLastOutFileTable).ToTable()
//	id := state.env.Get(ScopeKeyTmpFileID).ToInt()
//	tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, id))
//	randChildTable := tbl.childTables[rand.Intn(len(tbl.childTables))]
//	return Strs("load data local infile", fmt.Sprintf("'%s'", tmpFile), "into table", randChildTable.Name)
//})

//var PrepareStmt = NewFn(func(state *State) Fn {
//	prepare := GenNewPrepare(state.AllocGlobalID(ScopeKeyPrepareID))
//	state.AppendPrepare(prepare)
//	state.env.Put(ScopeKeyCurrentPrepare, prepare)
//	return And(
//		Str("prepare"),
//		Str(prepare.Name),
//		Str("from"),
//		Str(`"""`),
//		Query,
//		Str(`"""`))
//})
//
//var DeallocPrepareStmt = NewFn(func(state *State) Fn {
//	prepare := state.GetRandPrepare()
//	state.RemovePrepare(prepare)
//	return Strs("deallocate prepare", prepare.Name)
//})
//
//var QueryPrepare = NewFn(func(state *State) Fn {
//	Assert(len(state.prepareStmts) > 0, state)
//	prepare := state.GetRandPrepare()
//	assignments := prepare.GenAssignments()
//	if len(assignments) == 0 {
//		return Str(fmt.Sprintf("execute %s", prepare.Name))
//	}
//	for i := 1; i < len(assignments); i++ {
//		state.InjectTodoSQL(assignments[i])
//	}
//	userVarsStr := strings.Join(prepare.UserVars(), ",")
//	state.InjectTodoSQL(fmt.Sprintf("execute %s using %s", prepare.Name, userVarsStr))
//	return Str(assignments[0])
//})
