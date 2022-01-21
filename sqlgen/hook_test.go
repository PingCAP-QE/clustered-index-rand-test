package sqlgen_test

import (
	"fmt"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestHookPredNeedRollBackStmt(t *testing.T) {
	state := sqlgen.NewState()
	predHook := sqlgen.NewFnHookPred()
	rollBackStmts := []sqlgen.Fn{
		sqlgen.DDLStmt, sqlgen.FlashBackTable, sqlgen.CreateTable, sqlgen.SplitRegion,
	}
	for _, f := range rollBackStmts {
		predHook.AddMatchFn(f)
	}
	state.Hook().Append(predHook)

	for i := 0; i < 100; i++ {
		query := sqlgen.Start.Eval(state)
		if predHook.Matched() {
			fmt.Println(query)
		}
		predHook.ResetMatched()
	}
}

func TestHookReplacer(t *testing.T) {
	replacerHook := sqlgen.NewFnHookReplacer()
	query := "select sleep(100000000000);"
	hijacker := sqlgen.NewFn(func(state *sqlgen.State) sqlgen.Fn {
		return sqlgen.Str(query)
	})
	replacerHook.Replace(sqlgen.Start, hijacker)

	state := sqlgen.NewState()
	state.Hook().Append(replacerHook)
	result := sqlgen.Start.Eval(state)
	require.Equal(t, result, query)
}
