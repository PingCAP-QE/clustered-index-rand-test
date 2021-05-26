package sqlgen_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C) {
	rand.Seed(10086)
}

func (s *testSuite) TestStart(c *C) {
	state := sqlgen.NewState()
	for i := 0; i < 300; i++ {
		res := sqlgen.Start.Eval(state)
		c.Assert(len(res) > 0, IsTrue, Commentf(state.LastBrokenAssumption()))
		c.Assert(len(res), Greater, 0, Commentf("i = %d", i))
	}
}

func (s *testSuite) TestCreateColumnTypes(c *C) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyIntMaxTableCount, 100)
	state.StoreConfig(sqlgen.ConfigKeyArrayAllowColumnTypes, []sqlgen.ColumnType{sqlgen.ColumnTypeInt})
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
	intColCount := 0
	for i := 0; i < 100; i++ {
		res := sqlgen.CreateTable.Eval(state)
		c.Assert(len(res) > 0, IsTrue, Commentf(state.LastBrokenAssumption()))
		intColCount += strings.Count(res, "int")
	}
	c.Assert(intColCount, Equals, 100*5)
}

func (s *testSuite) TestCreateTableLike(c *C) {
	state := sqlgen.NewState()
	_ = sqlgen.CreateTable.Eval(state)
	for i := 0; i < 100; i++ {
		_ = sqlgen.CreateTableLike.Eval(state)
		state.CreateScope()
		state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{state.GetAllTables().PickOne()})
		sqlgen.AddColumn.Eval(state)
		state.DestroyScope()
		dropColTbls := state.FilterTables(func(t *sqlgen.Table) bool {
			state.CreateScope()
			defer state.DestroyScope()
			state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{t})
			return sqlgen.MoreThan1Columns(state) && sqlgen.HasDroppableColumn(state)
		})
		if len(dropColTbls) > 0 {
			state.CreateScope()
			state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{dropColTbls.PickOne()})
			sqlgen.DropColumn.Eval(state)
			state.DestroyScope()
		}
	}
	state.CheckIntegrity(state)
}

func (s *testSuite) TestAlterColumnPosition(c *C) {
	state := sqlgen.NewState()
	state.SetRepeat(sqlgen.ColumnDefinition, 10, 10)
	_ = sqlgen.CreateTable.Eval(state)
	state.CreateScope()
	state.Store(sqlgen.ScopeKeyCurrentTables, state.GetAllTables())
	defer state.DestroyScope()
	for i := 0; i < 10; i++ {
		_ = sqlgen.InsertInto.Eval(state)
	}
	for i := 0; i < 100; i++ {
		_ = sqlgen.AlterColumn.Eval(state)
	}
	// TODO: implement the type-value compatibility check.
	t := state.GetRandTable()
	for _, c := range t.Columns {
		fmt.Printf("%s ", c.Tp)
	}
	fmt.Println()
	for _, v := range t.GetRandRow(nil) {
		fmt.Printf("%s ", v)
	}
	fmt.Println()
}

func (s *testSuite) TestConfigKeyUnitAvoidAlterPKColumn(c *C) {
	state := sqlgen.NewState()
	state.SetRepeat(sqlgen.ColumnDefinition, 10, 10)
	state.SetRepeat(sqlgen.IndexDefinition, 1, 1)
	state.StoreConfig(sqlgen.ConfigKeyUnitAvoidAlterPKColumn, struct{}{})
	_ = sqlgen.CreateTable.Eval(state)
	tbl := state.GetRandTable()
	pk := tbl.GetRandomIndex()
	c.Assert(pk.Tp, Equals, sqlgen.IndexTypePrimary)
	colsWithPK := tbl.FilterColumns(func(c *sqlgen.Column) bool {
		return pk.ContainsColumn(c)
	})
	state.CreateScope()
	state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables{tbl})
	for i := 0; i < 30; i++ {
		sqlgen.AlterColumn.Eval(state)
	}
	for _, pkCol := range colsWithPK {
		c.Assert(tbl.ContainsColumn(pkCol), IsTrue)
	}
}
