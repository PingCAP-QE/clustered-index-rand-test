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
	return newGenerator(state)
}

// Separate newGenerator for tests.
func newGenerator(state *State) func() string {
	w := state.ctrl.Weight
	GenPlugins = append(GenPlugins, &ScopeListener{state: state})
	postListener := &PostListener{callbacks: map[string]func(){}}
	GenPlugins = append(GenPlugins, postListener)
	GenPlugins = append(GenPlugins, &DebugListener{})
	if state.ctrl.AttachToTxn {
		GenPlugins = append(GenPlugins, &TxnListener{ctl: state.ctrl})
	}
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
		if s, ok := state.PopOneTodoSQL(); ok {
			return Str(s)
		}

		if w.CTEJustSyntax {
			state.Store(ScopeKeyCTESyntaxExprDeep, NewScopeObj(0))
			state.Store(ScopeKeyCTESyntaxBodyDeep, NewScopeObj(0))
			return synQueryExpression
		}

		if state.IsInitializing() {
			return initStart
		}

		if w.MustCTE {
			return cteStart
		}
		return Or(
			switchRowFormatVer.SetW(w.SetRowFormat),
			switchClustered.SetW(w.SetClustered),
			adminCheck.SetW(w.AdminCheck),
			If(len(state.tables) < state.ctrl.MaxTableNum,
				Or(
					createTable.SetW(w.CreateTable_WithoutLike),
					createTableLike,
				),
			).SetW(w.CreateTable),
			If(len(state.tables) > 0,
				Or(
					dmlStmt.SetW(w.Query_DML),
					ddlStmt.SetW(w.Query_DDL),
					splitRegion.SetW(w.Query_Split),
					commonAnalyze.SetW(w.Query_Analyze),
					prepareStmt.SetW(w.Query_Prepare),
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
			).SetW(w.Query),
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
			query.SetW(w.Query_Select),
			If(len(state.prepareStmts) > 0,
				queryPrepare,
			),
			commonDelete.SetW(w.Query_DML_DEL),
			commonInsert.SetW(w.Query_DML_INSERT),
			commonUpdate.SetW(w.Query_DML_UPDATE),
		)
	})

	ddlStmt = NewFn("ddlStmt", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		return Or(
			addColumn,
			addIndex,
			If(len(tbl.Columns) > 1 && tbl.HasDroppableColumn(),
				dropColumn,
			),
			If(len(tbl.Indices) > 0,
				dropIndex,
			),
			If(state.ctrl.EnableColumnTypeChange,
				alterColumn,
			),
		)
	})

	switchRowFormatVer = NewFn("switchRowFormat", func() Fn {
		if RandomBool() {
			return Str("set @@global.tidb_row_format_version = 2")
		}
		return Str("set @@global.tidb_row_format_version = 1")
	})

	switchClustered = NewFn("switchClustered", func() Fn {
		if RandomBool() {
			state.enabledClustered = false
			return Str("set @@global.tidb_enable_clustered_index = 0")
		}
		state.enabledClustered = true
		return Str("set @@global.tidb_enable_clustered_index = 1")
	})

	dropTable = NewFn("dropTable", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyLastDropTable, NewScopeObj(tbl))
		return Strs("drop table", tbl.Name)
	})

	flashBackTable = NewFn("flashBackTable", func() Fn {
		tbl := state.GetRandTable()
		state.InjectTodoSQL(fmt.Sprintf("flashback table %s", tbl.Name))
		return Or(
			Strs("drop table", tbl.Name),
			Strs("truncate table", tbl.Name),
		)
	})

	adminCheck = NewFn("adminCheck", func() Fn {
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

	createTable = NewFn("createTable", func() Fn {
		tbl := GenNewTable(state.AllocGlobalID(ScopeKeyTableUniqID))
		state.AppendTable(tbl)
		postListener.Register("createTable", func() {
			tbl.ReorderColumns()
		})
		colDefs = NewFn("colDefs", func() Fn {
			colDef = NewFn("colDef", func() Fn {
				col := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID), w)
				tbl.AppendColumn(col)
				return And(Str(col.Name), Str(PrintColumnType(col)))
			})
			if state.IsInitializing() {
				return Repeat(colDef, state.ctrl.InitColCount, Str(","))
			}
			return Or(
				colDef,
				And(colDef, Str(","), colDefs).SetW(w.CreateTable_MoreCol),
			)
		})
		idxDefs = NewFn("idxDefs", func() Fn {
			idxDef = NewFn("idxDef", func() Fn {
				idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl, w)
				if idx.IsUnique() {
					partitionedCol := state.Search(ScopeKeyCurrentPartitionColumn)
					if !partitionedCol.IsNil() {
						// all partitioned Columns should be contained in every unique/primary index.
						c := partitionedCol.ToColumn()
						Assert(c != nil)
						idx.AppendColumnIfNotExists(c)
					}
				}
				tbl.AppendIndex(idx)
				clusteredKeyword := "/*T![clustered_index] clustered */"
				return And(
					Str(PrintIndexType(idx)),
					Str("key"),
					Str(idx.Name),
					Str("("),
					Str(PrintIndexColumnNames(idx)),
					Str(")"),
					OptIf(idx.Tp == IndexTypePrimary && w.CreateTable_WithClusterHint, Str(clusteredKeyword)),
				)
			})
			return RepeatRange(1, w.CreateTable_IndexMoreCol, idxDef, Str(","))
		})

		partitionDef = NewFn("partitionDef", func() Fn {
			partitionedCol := tbl.GetRandColumnForPartition()
			if partitionedCol == nil {
				return Empty()
			}
			state.StoreInParent(ScopeKeyCurrentPartitionColumn, NewScopeObj(partitionedCol))
			tbl.AppendPartitionColumn(partitionedCol)
			const hashPart, rangePart, listPart = 0, 1, 2
			randN := rand.Intn(6)
			switch w.CreateTable_Partition_Type {
			case "hash":
				randN = hashPart
			case "list":
				randN = listPart
			case "range":
				randN = rangePart
			}
			switch randN {
			case hashPart:
				partitionNum := RandomNum(1, 6)
				return And(
					Str("partition by"),
					Str("hash("),
					Str(partitionedCol.Name),
					Str(")"),
					Str("partitions"),
					Str(partitionNum),
				)
			case rangePart:
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
			case listPart:
				listVals := partitionedCol.RandomValuesAsc(20)
				listGroups := RandomGroups(listVals, rand.Intn(3)+1)
				return Strs(
					"partition by",
					"list(",
					partitionedCol.Name,
					") (",
					PrintListPartitionDefs(listGroups),
					")",
				)
			default:
				return Empty()
			}
		})
		PreEvalWithOrder(&colDefs, &partitionDef, &idxDefs)
		if state.ctrl.EnableTestTiFlash {
			state.InjectTodoSQL(fmt.Sprintf("alter table %s set tiflash replica 1", tbl.Name))
			state.InjectTodoSQL(fmt.Sprintf("select sleep(20)"))
		}
		indexOpt := 10
		if w.Query_INDEX_MERGE {
			indexOpt = 1000000
		}
		return And(
			Str("create table"),
			Str(tbl.Name),
			Str("("),
			colDefs,
			OptIf(rand.Intn(indexOpt) != 0 && !w.MustCTE,
				And(
					Str(","),
					idxDefs,
				),
			),
			Str(")"),
			OptIf(!w.MustCTE, partitionDef),
		)
	})

	insertInto = NewFn("insertInto", func() Fn {
		tbl := state.GetFirstNonFullTable()
		vals := tbl.GenRandValues(tbl.Columns)
		if w.MustCTE {
			vals = tbl.GenRandValuesForCTE(tbl.Columns)
		}
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
		forUpdateOpt = NewFn("forUpdateOpt", func() Fn {
			return Opt(Str("for update"))
		})
		union = NewFn("union", func() Fn {
			return Or(
				Str("union"),
				Str("union all"),
				Str("except"),
				Str("intersect"),
			)
		})
		aggSelect = NewFn("aggSelect", func() Fn {
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
		windowSelect = NewFn("windowSelect", func() Fn {
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

	commonInsert = NewFn("commonInsert", func() Fn {
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

		onDuplicateUpdate = NewFn("onDuplicateUpdate", func() Fn {
			cols := tbl.GetRandColumnsNonEmpty()

			return Or(
				Empty().SetW(3),
				Strs(
					"on duplicate key update",
					PrintRandomAssignments(cols),
				).SetW(w.Query_DML_INSERT_ON_DUP),
			)
		})

		onDupAssignment = NewFn("onDupAssignment", func() Fn {
			randCol := tbl.GetRandColumn()
			return Or(
				Strs(randCol.Name, "=", randCol.RandomValue()),
				Strs(randCol.Name, "=", randCol.Name),
			)
		})

		multipleRowVals = NewFn("multipleRowVals", func() Fn {
			vals := tbl.GenRandValues(cols)
			return Or(
				Strs("(", PrintRandValues(vals), ")").SetW(3),
				And(Strs("(", PrintRandValues(vals), ")"), Str(","), multipleRowVals),
			)
		})

		insertSetStmt = NewFn("insertAssignment", func() Fn {
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
			//	Str(PrintColumnNamesWithPar(Cols, "")),
			//	commonSelect,
			//	OptIf(insertOrReplace == "insert", onDuplicateUpdate),
			//),
		)
	})

	commonUpdate = NewFn("commonUpdate", func() Fn {
		tbl := state.GetRandTable()
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))
		orderByCols := tbl.GetRandColumns()

		updateAssignments = NewFn("updateAssignments", func() Fn {
			return Or(
				updateAssignment,
				And(updateAssignment, Str(","), updateAssignments),
			)
		})

		updateAssignment = NewFn("updateAssignment", func() Fn {
			randCol := tbl.GetRandColumn()
			return Or(
				Strs(randCol.Name, "=", randCol.RandomValue()),
			)
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

	commonAnalyze = NewFn("commonAnalyze", func() Fn {
		tbl := state.GetRandTable()
		return And(Str("analyze table"), Str(tbl.Name))
	})

	commonDelete = NewFn("commonDelete", func() Fn {
		tbl := state.GetRandTable()
		var col *Column
		if rand.Intn(w.Query_DML_DEL_COMMON+w.Query_DML_DEL_INDEX) < w.Query_DML_DEL_COMMON {
			col = tbl.GetRandColumn()
		} else {
			col = tbl.GetRandIndexFirstColumnWithWeight(w.Query_DML_DEL_INDEX_COMMON, w.Query_DML_DEL_INDEX_COMMON)
		}
		state.Store(ScopeKeyCurrentTable, NewScopeObj(tbl))

		multipleRowVal = NewFn("multipleRowVal", func() Fn {
			return Or(
				Str(col.RandomValue()).SetW(3),
				And(Str(col.RandomValue()), Str(","), multipleRowVal),
			)
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

	predicates = NewFn("predicates", func() Fn {
		tbl := getTable(state)
		uniqueIdx := tbl.GetRandUniqueIndexForPointGet()

		if w.Query_INDEX_MERGE {
			andPredicates := NewFn("andPredicates", func() Fn {
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

		predicatesPointGet = NewFn("predicatesPointGet", func() Fn {
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
			predicate.SetW(3),
			And(predicate, Or(Str("and"), Str("or")), predicates),
			If(uniqueIdx != nil, predicatesPointGet).SetW(1),
		)
	})

	predicate = NewFn("predicate", func() Fn {
		tbl := getTable(state)
		randCol := tbl.GetRandColumn()
		if w.Query_INDEX_MERGE {
			if len(tbl.colForPrefixIndex) > 0 {
				randCol = tbl.colForPrefixIndex[0]
				tbl.colForPrefixIndex = tbl.colForPrefixIndex[1:]
			}
		}
		randVal = NewFn("randVal", func() Fn {
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
		randColVals = NewFn("randColVals", func() Fn {
			return Or(
				randVal,
				And(randVal, Str(","), randColVals).SetW(3),
			)
		})
		colName := fmt.Sprintf("%s.%s", tbl.Name, randCol.Name)
		return Or(
			And(Str(colName), cmpSymbol, randVal),
			And(Str(colName), Str("in"), Str("("), randColVals, Str(")")),
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
		// Return empty because the limit is not friendly to clustered index AB test.
		return Empty()
	})

	addIndex = NewFn("addIndex", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		idx := GenNewIndex(state.AllocGlobalID(ScopeKeyIndexUniqID), tbl, w)
		tbl.AppendIndex(idx)

		return Strs(
			"alter table", tbl.Name,
			"add index", idx.Name,
			"(", PrintIndexColumnNames(idx), ")",
		)
	})

	dropIndex = NewFn("dropIndex", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		idx := tbl.GetRandomIndex()
		tbl.RemoveIndex(idx)
		return Strs(
			"alter table", tbl.Name,
			"drop index", idx.Name,
		)
	})

	addColumn = NewFn("addColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID), w)
		tbl.AppendColumn(col)
		return Strs(
			"alter table", tbl.Name,
			"add column", col.Name, PrintColumnType(col),
		)
	})

	dropColumn = NewFn("dropColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := tbl.GetRandDroppableColumn()
		tbl.RemoveColumn(col)
		return Strs(
			"alter table", tbl.Name,
			"drop column", col.Name,
		)
	})

	alterColumn = NewFn("alterColumn", func() Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		col := tbl.GetRandColumn()
		newCol := GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID), w)
		tbl.ReplaceColumn(col, newCol)
		const modify, change = false, true
		switch RandomBool() {
		case modify:
			newCol.Name = col.Name
			return Strs("alter table", tbl.Name, "modify column", col.Name, PrintColumnType(newCol))
		case change:
			return Strs("alter table", tbl.Name, "change column", col.Name, newCol.Name, PrintColumnType(newCol))
		}
		return NeverReach()
	})

	multiTableQuery = NewFn("multiTableQuery", func() Fn {
		tbl1 := state.GetRandTable()
		tbl2 := state.GetRandTable()
		cols1 := tbl1.GetRandColumns()
		cols2 := tbl2.GetRandColumns()
		state.Store(ScopeKeyCurrentMultiTable, NewScopeObj([]*Table{tbl1, tbl2}))
		preferIndex := RandomBool()

		joinPredicates = NewFn("joinPredicates", func() Fn {
			return Or(
				joinPredicate,
				And(joinPredicate, Or(Str("and"), Str("or")), joinPredicates),
			)
		})

		joinPredicate = NewFn("joinPredicate", func() Fn {
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
		joinHint = NewFn("joinHint", func() Fn {
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

		semiJoinStmt = NewFn("semiJoinStmt", func() Fn {
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
		return Strs("create table", newTbl.Name, "like", tbl.Name)
	})

	selectIntoOutFile = NewFn("selectIntoOutFile", func() Fn {
		tbl := state.GetRandTable()
		state.StoreInRoot(ScopeKeyLastOutFileTable, NewScopeObj(tbl))
		tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, state.AllocGlobalID(ScopeKeyTmpFileID)))
		return Strs("select * from", tbl.Name, "into outfile", fmt.Sprintf("'%s'", tmpFile))
	})

	loadTable = NewFn("loadTable", func() Fn {
		tbl := state.Search(ScopeKeyLastOutFileTable).ToTable()
		id := state.Search(ScopeKeyTmpFileID).ToInt()
		tmpFile := path.Join(SelectOutFileDir, fmt.Sprintf("%s_%d.txt", tbl.Name, id))
		randChildTable := tbl.childTables[rand.Intn(len(tbl.childTables))]
		return Strs("load data local infile", fmt.Sprintf("'%s'", tmpFile), "into table", randChildTable.Name)
	})

	splitRegion = NewFn("splitRegion", func() Fn {
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
		splitTableRegionBetween = NewFn("splitTableRegionBetween", func() Fn {
			rows := tbl.GenMultipleRowsAscForHandleCols(2)
			low, high := rows[0], rows[1]
			return Strs(splitTablePrefix, "between",
				"(", PrintRandValues(low), ")", "and",
				"(", PrintRandValues(high), ")", "regions", RandomNum(2, 10))
		})

		// split table t index idx between (1, 2) and (100, 200) regions 2;
		splitIndexRegionBetween = NewFn("splitIndexRegionBetween", func() Fn {
			rows := tbl.GenMultipleRowsAscForIndexCols(2, idx)
			low, high := rows[0], rows[1]
			return Strs(splitTablePrefix, idxPrefix, "between",
				"(", PrintRandValues(low), ")", "and",
				"(", PrintRandValues(high), ")", "regions", RandomNum(2, 10))
		})

		// split table t by ((1, 2), (100, 200));
		splitTableRegionBy = NewFn("splitTableRegionBy", func() Fn {
			rows := tbl.GenMultipleRowsAscForHandleCols(rand.Intn(10) + 2)
			return Strs(splitTablePrefix, "by", PrintSplitByItems(rows))
		})

		// split table t index idx by ((1, 2), (100, 200));
		splitIndexRegionBy = NewFn("splitIndexRegionBy", func() Fn {
			rows := tbl.GenMultipleRowsAscForIndexCols(rand.Intn(10)+2, idx)
			return Strs(splitTablePrefix, idxPrefix, "by", PrintSplitByItems(rows))
		})

		if splittingIndex {
			return Or(splitIndexRegionBetween, splitIndexRegionBy)
		}
		return Or(splitTableRegionBetween, splitTableRegionBy)
	})

	prepareStmt = NewFn("prepareStmt", func() Fn {
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

	deallocPrepareStmt = NewFn("deallocPrepareStmt", func() Fn {
		Assert(len(state.prepareStmts) > 0, state)
		prepare := state.GetRandPrepare()
		state.RemovePrepare(prepare)
		return Strs("deallocate prepare", prepare.Name)
	})

	queryPrepare = NewFn("queryPrepare", func() Fn {
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

	cteStart = NewFn("cteStart", func() Fn {
		state.IncCTEDeep()
		return Or(
			And(withClause, simpleCTEQuery),
		)
	})

	simpleCTEQuery = NewFn("simpleCTEQuery", func() Fn {
		parentCTEColCount := state.ParentCTEColCount()
		ctes := state.PopCTE()
		cteNames := make([]string, len(ctes))
		colNames := make([]string, 0, len(ctes)*2)
		for i := range ctes {
			cteNames[i] = ctes[i].Name
			for _, col := range ctes[i].Cols {
				colNames = append(colNames, col.Name)
			}
		}

		rand.Shuffle(len(colNames[1:]), func(i, j int) {
			colNames[1+i], colNames[1+j] = colNames[1+j], colNames[1+i]
		})
		// todo: it can infer the cte or the common table
		field := "*"
		if parentCTEColCount != 0 {
			field = strings.Join(colNames[:parentCTEColCount], ",")
		}
		return And(
			Str("select"),
			Str(field),
			Str("from"),
			Str(strings.Join(cteNames, ",")),
		)
	})

	withClause = NewFn("with clause", func() Fn {
		return And(
			Str("with"),
			IfElse(
				rand.Intn(100) < w.CTEValidSQL,
				Str("recursive"),
				Opt(Str("recursive")),
			),
			withList,
		)
	})

	withList = NewFn("with list", func() Fn {
		return Or(
			cte.SetW(100-w.CTEMultiRatio),
			And(cte, Str(","), withList).SetW(w.CTEMultiRatio),
		)
	})

	cte = NewFn("commonTableExpr", func() Fn {
		cte := GenNewCTE(state.AllocGlobalID(ScopeKeyCTEUniqID))
		colCnt := state.ParentCTEColCount()
		if colCnt == 0 {
			colCnt = 2
		}
		for i := 0; i < colCnt+rand.Intn(5); i++ {
			cte.AppendColumn(GenNewColumn(state.AllocGlobalID(ScopeKeyColumnUniqID), w))
		}
		if !ShouldValid(w.CTEValidSQL) {
			if RandomBool() && state.GetCTECount() != 0 {
				cte.Name = state.GetRandomCTE().Name
			} else {
				cte.Name = state.GetRandTable().Name
			}
		}
		state.PushCTE(cte)

		return And(
			Str(cte.Name),
			Str("("+PrintColumnNamesWithoutPar(cte.Cols, "")+")"),
			Str("AS"),
			queryExpressionParens,
		)
	})

	queryExpressionParens = NewFn("queryExpressionParens", func() Fn {
		cteSeedPart := NewFn("cteSeedPart", func() Fn {
			tbl := state.GetRandTable()
			currentCTE := state.CurrentCTE()
			fields := make([]string, len(currentCTE.Cols)-1)
			if !ShouldValid(w.CTEValidSQL) {
				fields = append(fields, make([]string, rand.Intn(3))...)
			}
			for i := range fields {
				if rand.Intn(3) == 0 {
					fields[i] = tbl.GetRandColumn().Name
				} else {
					if RandomBool() {
						fields[i] = fmt.Sprintf("%d", i+2)
					} else {
						if ShouldValid(w.CTEValidSQL) {
							fields[i] = fmt.Sprintf("cast(\"%d\" as char(20))", i+2)
						} else {
							fields[i] = fmt.Sprintf("\"%d\"", i+2)
						}
					}
				}
			}

			return Or(
				And(
					Str("select 1,"),
					Str(strings.Join(fields, ",")),
					Str("from"),
					Str(tbl.Name),
				).SetW(w.CTESimpleSeed),
				cteStart,
			)
		})

		cteRecursivePart := NewFn("cteRecursivePart", func() Fn {
			lastCTE := state.CurrentCTE()
			if !ShouldValid(w.CTEValidSQL) {
				lastCTE = state.GetRandomCTE()
			}
			fields := append(make([]string, 0, len(lastCTE.Cols)), fmt.Sprintf("%s + 1", lastCTE.Cols[0].Name))
			for _, col := range lastCTE.Cols[1:] {
				fields = append(fields, PrintColumnWithFunction(col))
			}
			if !ShouldValid(w.CTEValidSQL) && rand.Intn(20) == 0 {
				fields = append(fields, "1")
			}

			// todo: recursive part can be a function, const
			return Or(
				And(
					Str("select"),
					Str(strings.Join(fields, ",")),
					Str("from"),
					Str(lastCTE.Name), // todo: it also can be a cte
					Str("where"),
					Str(fmt.Sprintf("%s < %d", lastCTE.Cols[0].Name, w.CTERecursiveDeep)),
				),
				//Str("select 3, 4"),
			)
		})

		cteBody := NewFn("cteBody", func() Fn {
			return And(
				cteSeedPart,
				Opt(
					And(
						Str("UNION"),
						unionOption,
						cteRecursivePart,
					),
				),
			)
		})
		return Or(
			And(Str("("), cteBody, Str(")")),
		)
	})

	unionOption = NewFn("unionOption", func() Fn {
		return Or(
			Empty(),
			Str("DISTINCT"),
			Str("ALL"),
		)
	})

	synQueryExpression = NewFn("synQueryExpression", func() Fn {
		weight := state.Search(ScopeKeyCTESyntaxExprDeep).ToInt()
		state.Store(ScopeKeyCTESyntaxExprDeep, NewScopeObj(weight+1))
		return Or(
			synQueryExpressionBody.SetW(weight*3),
			And(synWithClause, synQueryExpressionBody),
			synQueryExpressionParens,
			And(synWithClause, synQueryExpressionParens),
		)
	})

	synQueryExpressionBody = NewFn("synQueryExpressionBody", func() Fn {
		weight := state.Search(ScopeKeyCTESyntaxBodyDeep).ToInt()
		state.Store(ScopeKeyCTESyntaxBodyDeep, NewScopeObj(weight+1))
		return Or(
			synQueryPrimary.SetW(weight*3),
			And(synQueryExpressionBody, Str("union"), unionOption, synQueryPrimary),
			And(synQueryExpressionParens, Str("union"), unionOption, synQueryPrimary),
			And(synQueryExpressionBody, Str("union"), unionOption, synQueryExpressionParens),
			And(synQueryExpressionParens, Str("union"), unionOption, synQueryExpressionParens),
		)
	})

	synQueryPrimary = NewFn("synQueryPrimary", func() Fn {
		return Str("select 1")
	})

	synWithListClause = NewFn("synWithListClause", func() Fn {
		return Or(
			And(synWithListClause, Str(","), synCTE),
			synCTE,
		)
	})

	synCTE = NewFn("synCTE", func() Fn {
		return And(
			Str("cte as "),
			synQueryExpressionParens,
		)
	})

	synWithClause = NewFn("synWithClause", func() Fn {
		return And(
			Str("with"),
			Opt(Str("recursive")),
			synWithListClause,
		)
	})

	synQueryExpressionParens = NewFn("synQueryExpressionParens", func() Fn {
		return Or(
			And(Str("("), synQueryExpressionParens, Str(")")),
			And(Str("("), synQueryExpression, Str(")")).SetW(4),
		)
	})

	return retFn
}

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
