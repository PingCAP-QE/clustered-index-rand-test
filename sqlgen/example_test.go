package sqlgen_test

import (
	"fmt"
	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
	"testing"
)

func (s *testSuite) TestExampleInitialize(c *C) {
	state := sqlgen.NewState()
	tableCount, columnCount := 5, 5
	indexCount, rowCount := 2, 10
	state.SetRepeat(sqlgen.ColumnDefinition, columnCount, columnCount)
	state.SetRepeat(sqlgen.IndexDefinition, indexCount, indexCount)
	for i := 0; i < tableCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
		c.Assert(state.Valid(), IsTrue, Commentf(state.LastBrokenAssumption()))
	}
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			fmt.Println(sql)
			c.Assert(state.Valid(), IsTrue, Commentf(state.LastBrokenAssumption()))
		}
		state.DestroyScope()
	}
}

func TestExampleCTE(t *testing.T) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyArrayAllowColumnTypes, []sqlgen.ColumnType{sqlgen.ColumnTypeChar, sqlgen.ColumnTypeInt})
	state.StoreConfig(sqlgen.ConfigKeyCTEValidSQLPercent, 100)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
	rowCount := 10
	tblCount := 2
	for i := 0; i < tblCount; i++ {
		sql := sqlgen.CreateTable.Eval(state)
		fmt.Println(sql)
		//.Assert(state.Valid(), IsTrue, Commentf(state.LastBrokenAssumption()))
	}
	for _, tb := range state.GetAllTables() {
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tb})
		for i := 0; i < rowCount; i++ {
			sql := sqlgen.InsertInto.Eval(state)
			fmt.Println(sql)
			//c.Assert(state.Valid(), IsTrue, Commentf(state.LastBrokenAssumption()))
		}
		state.DestroyScope()
	}

	for i := 0; i < 1000; i++ {
		fmt.Println(sqlgen.CTEStartWrapper.Eval(state))
	}
}
