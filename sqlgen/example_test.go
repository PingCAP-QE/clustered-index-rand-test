package sqlgen_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestReadMeExample(t *testing.T) {
	state := sqlgen.NewState()
	state.Config().SetMaxTable(200)
	state.SetWeight(sqlgen.IndexDefinitions, 0)
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	for i := 0; i < 200; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Print(sql)
		fmt.Println(";")
	}
}

func TestQuery(t *testing.T) {
	state := sqlgen.NewState()
	rowCount := 10
	tblCount := 2
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
	}
	for _, tb := range state.Tables {
		state.Env().Table = tb
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			fmt.Println(sql)
		}
	}
	queries := generateQuery(state, rowCount)
	for _, sql := range queries {
		fmt.Println(sql)
	}
}

func TestExampleInitialize(t *testing.T) {
	state := sqlgen.NewState()
	tableCount, columnCount := 5, 5
	indexCount, rowCount := 2, 10
	initSQLs := generateCreateTable(state, tableCount, columnCount, indexCount)
	for _, sql := range initSQLs {
		fmt.Println(sql)
	}
	insertSQLs := generateInsertInto(state, rowCount)
	for _, sql := range insertSQLs {
		fmt.Println(sql)
	}
}

//func TestExampleCTE(t *testing.T) {
//	state := sqlgen.NewState()
//	state.SetWeight(sqlgen.IndexDefinitions, 0)
//	state.SetWeight(sqlgen.PartitionDefinition, 0)
//	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
//	rowCount := 10
//	tblCount := 2
//	for i := 0; i < tblCount; i++ {
//		sql := sqlgen.CreateTable.Eval(state)
//		fmt.Println(sql)
//	}
//
//	generateInsertInto(state, rowCount)
//
//	for i := 0; i < 100; i++ {
//		fmt.Println(sqlgen.CTEQueryStatement.Eval(state))
//	}
//
//	for i := 0; i < 100; i++ {
//		fmt.Println(sqlgen.CTEDMLStatement.Eval(state), ";")
//	}
//}

func TestExampleCreateTableWithoutIndexOrPartitions(t *testing.T) {
	state := sqlgen.NewState()
	state.Config().SetMaxTable(200)
	state.SetWeight(sqlgen.IndexDefinitions, 0)
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	for i := 0; i < 200; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
		require.Greater(t, len(sql), 0)
		require.NotContains(t, sql, "index")
		require.NotContains(t, sql, "partition")
	}
}

func TestExampleIntegerColumnTypeChange(t *testing.T) {
	state := sqlgen.NewState()
	state.ReplaceRule(sqlgen.ColumnDefinitionType, sqlgen.ColumnDefinitionTypesIntegers)
	state.SetWeight(sqlgen.PartitionDefinitionList, 0)
	createTables := generateCreateTable(state, 5, 10, 8)
	for _, sql := range createTables {
		fmt.Println(sql)
	}
	insertSQLs := generateInsertInto(state, 20)
	for _, sql := range insertSQLs {
		fmt.Println(sql)
	}
	state.SetWeight(sqlgen.DDLStmt, 20)
	state.SetWeight(sqlgen.AlterColumn, 10)
	alterTableCount := 0
	for i := 0; i < 200; i++ {
		sql := sqlgen.Start.Eval(state)
		require.Greater(t, len(sql), 0)
		fmt.Println(sql)
		if strings.Contains(sql, "alter table") {
			alterTableCount++
		}
	}
	fmt.Printf("Total alter table statements: %d\n", alterTableCount)
}

func TestExampleColumnTypeChangeWithGivenTypes(t *testing.T) {
	state := sqlgen.NewState()
	state.Config().SetMaxTable(100)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
	state.ReplaceRule(sqlgen.ColumnDefinitionTypeOnCreate, sqlgen.ColumnDefinitionTypesIntegerInt)
	state.ReplaceRule(sqlgen.ColumnDefinitionTypeOnAdd, sqlgen.ColumnDefinitionTypesIntegerBig)
	state.ReplaceRule(sqlgen.ColumnDefinitionTypeOnModify, sqlgen.ColumnDefinitionTypesIntegerTiny)

	for i := 0; i < 100; i++ {
		query := sqlgen.CreateTable.Eval(state)
		require.Equal(t, 5, strings.Count(query, "int"), query)
	}
	for i := 0; i < 20; i++ {
		state.Env().Table = state.Tables.Rand()
		query := sqlgen.AddColumn.Eval(state)
		require.Contains(t, query, "bigint", query)
	}
	for i := 0; i < 20; i++ {
		randTable := state.Tables.Rand()
		state.Env().Table = state.Tables.Rand()
		query := sqlgen.AlterColumn.Eval(state)
		if len(query) == 0 {
			pk := randTable.Indexes.Primary()
			if pk != nil {
				require.Len(t, randTable.Columns.Diff(pk.Columns), 0)
			}
		} else {
			require.Contains(t, query, "tinyint", query)
		}
	}
}

func TestGBKCharacters(t *testing.T) {
	state := sqlgen.NewState()
	state.Config().SetMaxTable(100)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
}

func TestExampleSubSelectFieldCompatible(t *testing.T) {
	state := sqlgen.NewState()
	state.ReplaceRule(sqlgen.SubSelect, sqlgen.SubSelectWithGivenTp)
	query := sqlgen.CreateTable.Eval(state)
	require.Greater(t, len(query), 0)
	tbl := state.Tables.Rand()
	state.Env().Table = tbl
	state.Env().Column = tbl.Columns.Rand()
	for i := 0; i < 100; i++ {
		pred := sqlgen.Predicate.Eval(state)
		fmt.Println(pred)
	}
}

func generateCreateTable(state *sqlgen.State, tblCount, colCount, idxCount int) []string {
	result := make([]string, 0, tblCount)
	state.SetRepeat(sqlgen.ColumnDefinition, colCount, colCount)
	state.SetRepeat(sqlgen.IndexDefinition, idxCount, idxCount)
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		result = append(result, sql)
	}
	return result
}

func generateInsertInto(state *sqlgen.State, rowCount int) []string {
	result := make([]string, 0, rowCount)
	for _, tb := range state.Tables {
		state.Env().Table = tb
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			result = append(result, sql)
		}
	}
	return result
}

func generateQuery(state *sqlgen.State, count int) []string {
	result := make([]string, 0, count)
	for _, tb := range state.Tables {
		state.Env().Table = tb
		for i := 0; i < count; i++ {
			sql := sqlgen.Query.Eval(state)
			result = append(result, sql)
		}
	}
	return result
}
