package cases

import (
	"fmt"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
)

func TestGBKCase(t *testing.T) {
	state := NewGBKState()
	state.ReplaceRule(sqlgen.ColumnDefinitionType,
		sqlgen.Or(
			sqlgen.ColumnDefinitionTypesStrings,
			sqlgen.ColumnDefinitionTypesIntegerInt,
		),
	)
	for i := 0; i < 100; i++ {
		query := sqlgen.Start.Eval(state)
		fmt.Printf("/*%d*/ ", i)
		fmt.Println(query)
	}
}
