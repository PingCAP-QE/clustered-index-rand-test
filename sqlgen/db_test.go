package sqlgen_test

import (
	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

func (s *testSuite) TestStateClear(c *C) {
	state := sqlgen.NewState()
	defer state.CheckIntegrity()
	state.SetWeight(sqlgen.CreateTable, 100)
	c.Assert(state.GetWeight(sqlgen.CreateTable), Equals, 100)
	state.Clear(sqlgen.StateClearOptionWeight)
	c.Assert(state.GetWeight(sqlgen.CreateTable), Equals, 1)
	state.CreateScope()
	state.Store(sqlgen.ScopeKeyCurrentTables, state.GenNewTable())
	state.Clear(sqlgen.StateClearOptionScope)
	c.Assert(state.Exists(sqlgen.ScopeKeyCurrentTables), IsFalse)
	state.SetRepeat(sqlgen.CreateTable, 1, 10)
	low, up := state.GetRepeat(sqlgen.CreateTable)
	c.Assert(low, Equals, 1)
	c.Assert(up, Equals, 10)
	state.Clear(sqlgen.StateClearOptionRepeat)
	low, up = state.GetRepeat(sqlgen.CreateTable)
	c.Assert(low, Equals, 1)
	c.Assert(up, Equals, 3)
}
