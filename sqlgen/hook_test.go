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
