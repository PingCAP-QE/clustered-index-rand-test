package sqlgen_test

import (
	"fmt"
	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

func (s *testSuite) TestHookPredNeedRollBackStmt(c *C) {
	state := sqlgen.NewState()
	predHook := sqlgen.NewFnHookPred()
	rollBackStmts := []sqlgen.Fn{
		sqlgen.DDLStmt, sqlgen.FlashBackTable, sqlgen.CreateTable, sqlgen.SplitRegion,
	}
	for _, f := range rollBackStmts {
		predHook.AddMatchFn(f)
	}
	state.AppendHook(predHook)

	for i := 0; i < 100; i++ {
		query := sqlgen.Start.Eval(state)
		if predHook.Matched() {
			fmt.Println(query)
		}
		predHook.ResetMatched()
	}
}

func (s *testSuite) TestHookReplacer(c *C) {
	replacerHook := sqlgen.NewFnHookReplacer()
	query := "select sleep(100000000000);"
	hijacker := sqlgen.NewFn(func(state *sqlgen.State) sqlgen.Fn {
		return sqlgen.Str(query)
	})
	replacerHook.Replace(sqlgen.Start, hijacker)

	state := sqlgen.NewState()
	state.AppendHook(replacerHook)
	result := sqlgen.Start.Eval(state)
	c.Assert(result, Equals, query)
}
