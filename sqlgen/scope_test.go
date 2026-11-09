package sqlgen_test

import (
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	_ "github.com/pingcap/tidb/parser/test_driver"
)

func TestGeneratedSettingsAreSessionScoped(t *testing.T) {
	state := sqlgen.NewState()
	for _, rule := range []sqlgen.Fn{sqlgen.SwitchRowFormatVer, sqlgen.SwitchClustered} {
		text, err := rule.Eval(state)
		if err != nil {
			t.Fatal(err)
		}
		statement, err := parser.New().ParseOneStmt(text, "", "")
		if err != nil {
			t.Fatal(err)
		}
		setting, ok := statement.(*ast.SetStmt)
		if !ok {
			t.Fatal("not a setting")
		}
		for _, variable := range setting.Variables {
			if variable.IsGlobal || !variable.IsSystem {
				t.Fatalf("instance-scoped setting: %s", text)
			}
		}
	}
}
