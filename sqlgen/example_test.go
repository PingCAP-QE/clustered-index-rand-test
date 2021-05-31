package sqlgen_test

import (
	"fmt"
	"strings"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

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
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			result = append(result, sql)
		}
		state.DestroyScope()
	}
	return result
}

func generateQuery(state *sqlgen.State, count int) []string {
	result := make([]string, 0, count)
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < count; i++ {
			sql := sqlgen.Query.Eval(state)
			result = append(result, sql)
		}
		state.DestroyScope()
	}
	return result
}

func (s *testSuite) TestQuery(c *C) {
	state := sqlgen.NewState()
	rowCount := 10
	tblCount := 2
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
	}
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			fmt.Println(sql)
		}
		state.DestroyScope()
	}
	queries := generateQuery(state, rowCount)
	for _, sql := range queries {
		fmt.Println(sql)
	}
}

func (s *testSuite) TestExampleInitialize(c *C) {
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

func (s *testSuite) TestExampleCTE(c *C) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyArrayAllowColumnTypes, []sqlgen.ColumnType{sqlgen.ColumnTypeChar, sqlgen.ColumnTypeInt})
	state.StoreConfig(sqlgen.ConfigKeyCTEValidSQLPercent, 100)
	state.SetWeight(sqlgen.IndexDefinitions, 0)
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
	rowCount := 10
	tblCount := 2
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
	}

	generateInsertInto(state, rowCount)

	for i := 0; i < 100; i++ {
		fmt.Println(sqlgen.CTEQueryStatement.Eval(state))
	}

	for i := 0; i < 100; i++ {
		fmt.Println(sqlgen.CTEDMLStatement.Eval(state), ";")
	}
}

func (s *testSuite) TestExampleCreateTableWithoutIndexOrPartitions(c *C) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyIntMaxTableCount, 200)
	state.SetWeight(sqlgen.IndexDefinitions, 0)
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	for i := 0; i < 200; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
		c.Assert(len(sql) > 0, IsTrue, Commentf(state.LastBrokenAssumption()))
		c.Assert(strings.Contains(sql, "index"), IsFalse)
		c.Assert(strings.Contains(sql, "partition"), IsFalse)
	}
}

func (s *testSuite) TestExampleIntegerColumnTypeChange(c *C) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyArrayAllowColumnTypes, sqlgen.ColumnTypeIntegerTypes)
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
		c.Assert(len(sql) > 0, IsTrue, Commentf(state.LastBrokenAssumption()))
		fmt.Println(sql)
		if strings.Contains(sql, "alter table") {
			alterTableCount++
		}
	}
	fmt.Printf("Total alter table statements: %d\n", alterTableCount)
}

func (s *testSuite) TestExampleColumnTypeChangeWithGivenTypes(c *C) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyIntMaxTableCount, 100)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)

	tpCfg := sqlgen.NewConfigAllowColumnTypes()
	tpCfg.CreateTable = []sqlgen.ColumnType{sqlgen.ColumnTypeInt}
	tpCfg.AddColumn = []sqlgen.ColumnType{sqlgen.ColumnTypeBigInt}
	tpCfg.ModifyColumn = []sqlgen.ColumnType{sqlgen.ColumnTypeTinyInt}
	state.StoreConfig(sqlgen.ConfigKeyArrayAllowColumnTypes, tpCfg)
	for i := 0; i < 100; i++ {
		query := sqlgen.CreateTable.Eval(state)
		c.Assert(strings.Count(query, "int"), Equals, 5, Commentf("%s", query))
	}
	for i := 0; i < 20; i++ {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{state.GetRandTable()})
		query := sqlgen.AddColumn.Eval(state)
		c.Assert(strings.Contains(query, "bigint"), IsTrue, Commentf("%s", query))
		state.DestroyScope()
	}
	for i := 0; i < 20; i++ {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{state.GetRandTable()})
		query := sqlgen.AlterColumn.Eval(state)
		c.Assert(strings.Contains(query, "tinyint"), IsTrue, Commentf("%s", query))
		state.DestroyScope()
	}
}
