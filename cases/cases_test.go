package cases

import (
	"fmt"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/stretchr/testify/require"
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
		query, err := sqlgen.Start.Eval(state)
		require.NoError(t, err)
		fmt.Printf("/*%d*/ ", i)
		fmt.Println(query)
	}
}

func TestTiDB600Case(t *testing.T) {
	state := NewStateForTiDB600()
	for i := 0; i < 1000; i++ {
		query, err := sqlgen.Start.Eval(state)
		require.NoError(t, err)
		fmt.Printf("/*%d*/ ", i)
		fmt.Println(query)
	}
}
