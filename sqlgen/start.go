package sqlgen

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/zyguan/sqlz/resultset"
	"log"
	"math/rand"
	"path"
	"strings"
	"time"
)

func NewGenerator(state *State) func() string {
	rand.Seed(time.Now().UnixNano())
	GenPlugins = append(GenPlugins, &ScopeListener{state: state})
	postListener := &PostListener{callbacks: map[string]func(){}}
	GenPlugins = append(GenPlugins, postListener)
	GenPlugins = append(GenPlugins, &DebugListener{})
	retFn := func() string {
		res := evaluateFn(start)
		switch res.Tp {
		case PlainString:
			return res.Value
		case Invalid:
			log.Println("Invalid SQL")
			return ""
		default:
			log.Fatalf("Unsupported result type '%v'", res.Tp)
			return ""
		}
	}

	start = NewFn("start", func() Fn {
		if sql, ok := state.PopOneTodoSQL(); ok {
			return Str(sql)
		}
		if state.IsInitializing() {
			return initStart
		}
		return Or(
			switchSysVars,
			adminCheck,
			If(len(state.tables) < state.ctrl.MaxTableNum,
				Or(
					createTable.SetW(4),
					createTableLike,
				),
			).SetW(13),
			If(len(state.tables) > 0,
				Or(
					dmlStmt.SetW(20),
					ddlStmt.SetW(5),
					//splitRegion.SetW(2),
					//commonAnalyze.SetW(2),
					prepareStmt.SetW(2),
					If(len(state.prepareStmts) > 0,
						deallocPrepareStmt,
					).SetW(1),
					If(state.ctrl.CanReadGCSavePoint,
						flashBackTable,
					).SetW(1),
					If(state.ctrl.EnableSelectOutFileAndLoadData,
						Or(
							selectIntoOutFile.SetW(1),
							If(!state.Search(ScopeKeyLastOutFileTable).IsNil(),
								loadTable,
							),
						),
					).SetW(1),
				),
			).SetW(15),
		)
	})

	initStart = NewFn("initStart", func() Fn {
		if len(state.tables) < state.ctrl.InitTableCount {
			return createTable
		} else {
			return insertInto
		}
	})

	dmlStmt = NewFn("dmlStmt", func() Fn {
		return Or(
			query,
			If(len(state.prepareStmts) > 0,
				queryPrepare,
			),
			//commonDelete,
			//commonInsert,
			//commonUpdate,
		)
	})

	ddlStmt = NewFn("ddlStmt", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		return Or(
			addColumn,
			addIndex,
			If(len(tbl.columns) > 1 && tbl.HasDroppableColumn(),
				dropColumn,
			),
			If(len(tbl.indices) > 0,
				dropIndex,
			),
		)
	})

	switchSysVars = NewFn("switchSysVars", func() Fn {
		if RandomBool() {
			if RandomBool() {
				return Str("set @@global.tidb_row_format_version = 2")
			}
			return Str("set @@global.tidb_row_format_version = 1")
		} else {
			if RandomBool() {
				state.enabledClustered = false
				return Str("set @@tidb_enable_clustered_index = 0")
			}
			state.enabledClustered = true
			return Str("set @@tidb_enable_clustered_index = 1")
		}
	})

	dropTable = NewFn("dropTable", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyLastDropTable, NewScopeObj(tbl))
		return Strs("drop table", tbl.name)
	})

	flashBackTable = NewFn("flashBackTable", func() Fn {
		tbl := state.GetRandTable()
		state.InjectTodoSQL(fmt.Sprintf("flashback table %s", tbl.name))
		return Or(
			Strs("drop table", tbl.name),
			Strs("truncate table", tbl.name),
		)
	})

	adminCheck = NewFn("adminCheck", func() Fn {
		tbl := state.GetRandTable()
		if state.ctrl.EnableTestTiFlash {
			// Error: Error 1815: Internal : Can't find a proper physical plan for this query
			// https://github.com/pingcap/tidb/issues/22947
			return Str("")
		} else {
			if len(tbl.indices) == 0 {
				return Strs("admin check table", tbl.name)
			}
			idx := tbl.GetRandomIndex()
			return Or(
				Strs("admin check table", tbl.name),
				Strs("admin check index", tbl.name, idx.name),
			)
		}
	})

	createTable = NewFn("createTable", func() Fn {
		tbl := GenNewTable(state.AllocGlobalID(ScopeKeyTableUniqID))
		state.AppendTable(tbl)
		postListener.Register("createTable", func() {
			tbl.ReorderColumns()
			tbl.SetPrimaryKeyAndHandle(state)
		})
		colDefs = NewFn("colDefs", func() Fn {
			colDef = NewFn("colDef", func() Fn {
				col := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID))
				tbl.AppendColumn(col)
				return And(Str(col.name), Str(PrintColumnType(col)))
			})
			if state.IsInitializing() {
				return Repeat(colDef, state.ctrl.InitColCount, Str(","))
			}
			return Or(
				colDef,
				And(colDef, Str(","), colDefs).SetW(2),
			)
		})
		idxDefs = NewFn("idxDefs", func() Fn {
			idxDef = NewFn("idxDef", func() Fn {
				idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
				if idx.IsUnique() {
					partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn)
					if !partitionedCol.IsNil() {
						// all partitioned columns should be contained in every unique/primary index.
						c := partitionedCol.ToColumn()
						Assert(c != nil)
						idx.AppendColumnIfNotExists(c)
					}
				}
				tbl.AppendIndex(idx)
				return And(
					Str(PrintIndexType(idx)),
					Str("key"),
					Str(idx.name),
					Str("("),
					Str(PrintIndexColumnNames(idx)),
					Str(")"),
				)
			})
			return Or(
				idxDef.SetW(1),
				And(idxDef, Str(","), idxDefs).SetW(2),
			)
		})

		partitionDef = NewFn("partitionDef", func() Fn {
			if rand.Intn(5) != 0 {
				return Empty()
			}
			partitionedCol := tbl.GetRandColumnForPartition()
			if partitionedCol == nil {
				return Empty()
			}
			state.StoreInParent(ScopeKeyCurrentPartitionColumn, NewScopeObj(partitionedCol))
			tbl.AppendPartitionColumn(partitionedCol)
			partitionNum := RandomNum(1, 6)
			return And(
				Str("partition by"),
				Str("hash("),
				Str(partitionedCol.name),
				Str(")"),
				Str("partitions"),
				Str(partitionNum),
			)
		})
		PreEvalWithOrder(&colDefs, &partitionDef, &idxDefs)
		if state.ctrl.EnableTestTiFlash {
			state.InjectTodoSQL(fmt.Sprintf("alter table %s set tiflash replica 1", tbl.name))
			state.InjectTodoSQL(fmt.Sprintf("select sleep(20)"))
		}
		return And(
			Str("create table"),
			Str(tbl.name),
			Str("("),
			colDefs,
			OptIf(rand.Intn(10) != 0,
				And(
					Str(","),
					idxDefs,
				),
			),
			Str(")"),
			partitionDef,
		)
	})

	insertInto = NewFn("insertInto", func() Fn {
		tbl := state.GetFirstNonFullTable()
		vals := tbl.GenRandValues(tbl.columns)
		tbl.AppendRow(vals)
		return And(
			Str("insert into"),
			Str(tbl.name),
			Str("values"),
			Str("("),
			Str(PrintRandValues(vals)),
			Str(")"),
		)
	})

	query = NewFn("query", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		cols := tbl.GetRandColumns()

		commonSelect = NewFn("commonSelect", func() Fn {
			prepare := state.Search(ScopeKeyCurrentPrepare)
			if !prepare.IsNil() {
				paramCols := SwapOutParameterizedColumns(cols)
				prepare.ToPrepare().AppendColumns(paramCols...)
			}
			return And(Str("select"),
				OptIf(state.ctrl.EnableTestTiFlash,
					And(
						Str("/*+ read_from_storage(tiflash["),
						Str(tbl.name),
						Str("]) */"),
					)),
				Str(PrintColumnNamesWithoutPar(cols, "*")),
				Str("from"),
				Str(tbl.name),
				Str("where"),
				predicate,
			)
		})
		forUpdateOpt = NewFn("forUpdateOpt", func() Fn {
			return Opt(Str("for update"))
		})
		union = NewFn("union", func() Fn {
			return Or(
				Str("union"),
				Str("union all"),
			)
		})
		aggSelect = NewFn("aggSelect", func() Fn {
			intCol := tbl.GetRandIntColumn()
			if intCol == nil {
				return And(
					Str("select"),
					OptIf(state.ctrl.EnableTestTiFlash,
						And(
							Str("/*+ read_from_storage(tiflash["),
							Str(tbl.name),
							Str("]) */"),
						)),
					Str("count(*) from"),
					Str(tbl.name),
					Str("where"),
					predicate,
				)
			}
			return Or(
				And(
					Str("select"),
					OptIf(state.ctrl.EnableTestTiFlash,
						And(
							Str("/*+ read_from_storage(tiflash["),
							Str(tbl.name),
							Str("]) */"),
						)),
					Str("count(*) from"),
					Str(tbl.name),
					Str("where"),
					predicate,
				),
				And(
					Str("select"),
					OptIf(state.ctrl.EnableTestTiFlash,
						And(
							Str("/*+ read_from_storage(tiflash["),
							Str(tbl.name),
							Str("]) */"),
						)),
					Str("sum("),
					Str(intCol.name),
					Str(")"),
					Str("from"),
					Str(tbl.name),
					Str("where"),
					predicate,
				),
			)
		})

		return Or(
			And(commonSelect, forUpdateOpt),
			And(
				Str("("), commonSelect, forUpdateOpt, Str(")"),
				union,
				Str("("), commonSelect, forUpdateOpt, Str(")"),
			),
			And(aggSelect, forUpdateOpt),
			And(
				Str("("), aggSelect, forUpdateOpt, Str(")"),
				union,
				Str("("), aggSelect, forUpdateOpt, Str(")"),
			),
			If(len(state.tables) > 1,
				multiTableQuery,
			),
		)
	})

	commonInsert = NewFn("commonInsert", func() Fn {
		tbl := state.GetRandTable()
		var cols []*Column
		if state.ctrl.StrictTransTable {
			cols = tbl.GetRandColumnsIncludedDefaultValue()
		} else {
			cols = tbl.GetRandColumns()
		}
		insertOrReplace := "insert"
		if rand.Intn(3) == 0 {
			insertOrReplace = "replace"
		}

		onDuplicateUpdate = NewFn("onDuplicateUpdate", func() Fn {
			return Or(
				Empty().SetW(3),
				And(
					Str("on duplicate key update"),
					Or(
						onDupAssignment.SetW(4),
						And(onDupAssignment, Str(","), onDupAssignment),
					),
				),
			)
		})

		onDupAssignment = NewFn("onDupAssignment", func() Fn {
			randCol := tbl.GetRandColumn()
			return Or(
				Strs(randCol.name, "=", randCol.RandomValue()),
				Strs(randCol.name, "=", "values(", randCol.name, ")"),
			)
		})

		multipleRowVals = NewFn("multipleRowVals", func() Fn {
			vals := tbl.GenRandValues(cols)
			return Or(
				Strs("(", PrintRandValues(vals), ")").SetW(3),
				And(Strs("(", PrintRandValues(vals), ")"), Str(","), multipleRowVals),
			)
		})

		return Or(
			And(
				Str(insertOrReplace),
				Str("into"),
				Str(tbl.name),
				Str(PrintColumnNamesWithPar(cols, "")),
				Str("values"),
				multipleRowVals,
				OptIf(insertOrReplace == "insert", onDuplicateUpdate),
			),
		)
	})

	commonUpdate = NewFn("commonUpdate", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		orderByCols := tbl.GetRandColumns()

		updateAssignment = NewFn("updateAssignment", func() Fn {
			randCol := tbl.GetRandColumn()
			return Or(
				Strs(randCol.name, "=", randCol.RandomValue()),
			)
		})

		return And(
			Str("update"),
			Str(tbl.name),
			Str("set"),
			updateAssignment,
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

	commonAnalyze = NewFn("commonAnalyze", func() Fn {
		tbl := state.GetRandTable()
		return And(Str("analyze table"), Str(tbl.name))
	})

	commonDelete = NewFn("commonDelete", func() Fn {
		tbl := state.GetRandTable()
		col := tbl.GetRandColumn()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))

		multipleRowVal = NewFn("multipleRowVal", func() Fn {
			return Or(
				Str(col.RandomValue()).SetW(3),
				And(Str(col.RandomValue()), Str(","), multipleRowVal),
			)
		})

		return And(
			Str("delete from"),
			Str(tbl.name),
			Str("where"),
			Or(
				And(predicates, maybeLimit),
				And(Str(col.name), Str("in"), Str("("), multipleRowVal, Str(")"), maybeLimit),
				And(Str(col.name), Str("is null"), maybeLimit),
			),
		)
	})

	predicates = NewFn("predicates", func() Fn {
		return Or(
			predicate.SetW(3),
			And(predicate, Or(Str("and"), Str("or")), predicates),
		)
	})

	predicate = NewFn("predicate", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		randCol := tbl.GetRandColumn()
		randVal = NewFn("randVal", func() Fn {
			var v string
			prepare := state.Search(ScopeKeyCurrentPrepare)
			if !prepare.IsNil() && rand.Intn(5) == 0 {
				prepare.ToPrepare().AppendColumns(randCol)
				v = "?"
			} else if rand.Intn(3) == 0 || len(tbl.values) == 0 {
				v = randCol.RandomValue()
			} else {
				v = tbl.GetRandRowVal(randCol)
			}
			return Str(v)
		})
		randColVals = NewFn("randColVals", func() Fn {
			return Or(
				randVal,
				And(randVal, Str(","), randColVals).SetW(3),
			)
		})
		return Or(
			And(Str(randCol.name), cmpSymbol, randVal),
			And(Str(randCol.name), Str("in"), Str("("), randColVals, Str(")")),
		)
	})

	cmpSymbol = NewFn("cmpSymbol", func() Fn {
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

	maybeLimit = NewFn("maybeLimit", func() Fn {
		return Or(
			Empty().SetW(3),
			Strs("limit", RandomNum(1, 10)),
		)
	})

	addIndex = NewFn("addIndex", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl)
		tbl.AppendIndex(idx)

		return Strs(
			"alter table", tbl.name,
			"add index", idx.name,
			"(", PrintIndexColumnNames(idx), ")",
		)
	})

	dropIndex = NewFn("dropIndex", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		idx := tbl.GetRandomIndex()
		tbl.RemoveIndex(idx)
		return Strs(
			"alter table", tbl.name,
			"drop index", idx.name,
		)
	})

	addColumn = NewFn("addColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID))
		tbl.AppendColumn(col)
		return Strs(
			"alter table", tbl.name,
			"add column", col.name, PrintColumnType(col),
		)
	})

	dropColumn = NewFn("dropColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := tbl.GetRandDroppableColumn()
		tbl.RemoveColumn(col)
		return Strs(
			"alter table", tbl.name,
			"drop column", col.name,
		)
	})

	multiTableQuery = NewFn("multiTableQuery", func() Fn {
		tbl1 := state.GetRandTable()
		tbl2 := state.GetRandTable()
		cols1 := tbl1.GetRandColumns()
		cols2 := tbl2.GetRandColumns()

		group := GroupColumnsByColumnTypes(tbl1, tbl2)
		group = FilterUniqueColumns(group)
		joinPredicates = NewFn("joinPredicates", func() Fn {
			return Or(
				joinPredicate,
				And(joinPredicate, Or(Str("and"), Str("or")), joinPredicates),
			)
		})

		joinPredicate = NewFn("joinPredicate", func() Fn {
			col1, col2 := RandColumnPairWithSameType(group)
			return And(
				Str(col1.name),
				cmpSymbol,
				Str(col2.name),
			)
		})
		if len(group) == 0 {
			return And(
				Str("select"),
				OptIf(state.ctrl.EnableTestTiFlash,
					And(
						Str("/*+ read_from_storage(tiflash["),
						Str(tbl1.name),
						Str(","),
						Str(tbl2.name),
						Str("]) */"),
					)),
				Str(PrintFullQualifiedColName(tbl1, cols1)),
				Str(","),
				Str(PrintFullQualifiedColName(tbl2, cols2)),
				Str("from"),
				Str(tbl1.name),
				Str("join"),
				Str(tbl2.name),
			)
		}

		return And(
			Str("select"),
			OptIf(state.ctrl.EnableTestTiFlash,
				And(
					Str("/*+ read_from_storage(tiflash["),
					Str(tbl1.name),
					Str(","),
					Str(tbl2.name),
					Str("]) */"),
				)),
			Str(PrintFullQualifiedColName(tbl1, cols1)),
			Str(","),
			Str(PrintFullQualifiedColName(tbl2, cols2)),
			Str("from"),
			Str(tbl1.name),
			Or(Str("left join"), Str("join"), Str("right join")),
			Str(tbl2.name),
			And(Str("on"), joinPredicates),
		)
	})

	createTableLike = NewFn("createTableLike", func() Fn {
		tbl := state.GetRandTable()
		newTbl := tbl.Clone(func() int {
			return state.AllocGlobalID(ScopeKeyTableUniqID)
		}, func() int {
			return state.AllocGlobalID(ScopeKeyColumnUniqID)
		}, func() int {
			return state.AllocGlobalID(ScopeKeyIndexUniqID)
		})
		state.AppendTable(newTbl)
		return Strs("create table", newTbl.name, "like", tbl.name)
	})

	selectIntoOutFile = NewFn("selectIntoOutFile", func() Fn {
		tbl := state.GetRandTable()
		state.StoreInRoot(ScopeKeyLastOutFileTable, NewScopeObj(tbl))
		tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.name, state.AllocGlobalID(ScopeKeyTmpFileID)))
		return Strs("select * from", tbl.name, "into outfile", fmt.Sprintf("'%s'", tmpFile))
	})

	loadTable = NewFn("loadTable", func() Fn {
		tbl := state.Search(ScopeKeyLastOutFileTable).ToTable()
		id := state.Search(ScopeKeyTmpFileID).ToInt()
		tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.name, id))
		randChildTable := tbl.childTables[rand.Intn(len(tbl.childTables))]
		return Strs("load data local infile", fmt.Sprintf("'%s'", tmpFile), "into table", randChildTable.name)
	})

	splitRegion = NewFn("splitRegion", func() Fn {
		tbl := state.GetRandTable()
		rows := tbl.GenMultipleRowsAscForHandleCols(2)
		row1, row2 := rows[0], rows[1]

		return Strs(
			"split table", tbl.name, "between",
			"(", PrintRandValues(row1), ")", "and",
			"(", PrintRandValues(row2), ")", "regions", RandomNum(2, 10))
	})

	prepareStmt = NewFn("prepareStmt", func() Fn {
		prepare := GenNewPrepare(state.AllocGlobalID(ScopeKeyPrepareID))
		state.AppendPrepare(prepare)
		state.Store(ScopeKeyCurrentPrepare, NewScopeObj(prepare))
		return And(
			Str("prepare"),
			Str(prepare.name),
			Str("from"),
			Str(`"`),
			query,
			Str(`"`))
	})

	deallocPrepareStmt = NewFn("deallocPrepareStmt", func() Fn {
		Assert(len(state.prepareStmts) > 0, state)
		prepare := state.GetRandPrepare()
		state.RemovePrepare(prepare)
		return Strs("deallocate prepare", prepare.name)
	})

	queryPrepare = NewFn("queryPrepare", func() Fn {
		Assert(len(state.prepareStmts) > 0, state)
		prepare := state.GetRandPrepare()
		assignments := prepare.GenAssignments()
		if len(assignments) == 0 {
			return Str(fmt.Sprintf("execute %s", prepare.name))
		}
		for i := 1; i < len(assignments); i++ {
			state.InjectTodoSQL(assignments[i])
		}
		userVarsStr := strings.Join(prepare.UserVars(), ",")
		state.InjectTodoSQL(fmt.Sprintf("execute %s using %s", prepare.name, userVarsStr))
		return Str(assignments[0])
	})
	return retFn
}

func RunInteractTest(ctx context.Context, db1, db2 *sql.DB, state *State, sql string) error {
	log.Printf("%s", sql)
	rs1, err1 := runQuery(ctx, db1, sql)
	rs2, err2 := runQuery(ctx, db2, sql)
	if !ValidateErrs(err1, err2) {
		return fmt.Errorf("errors mismatch: %v <> %v %q", err1, err2, sql)
	}
	if rs1 == nil || rs2 == nil {
		return nil
	}
	h1, h2 := rs1.DataDigest(), rs2.DataDigest()
	if h1 != h2 {
		return fmt.Errorf("result digests mismatch: %s != %s %q", h1, h2, sql)
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
		"for column '_tidb_rowid'",                       // 4.0 split table between may generate incorrect value.
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
