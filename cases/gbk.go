package cases

import "github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"

func NewGBKState() *sqlgen.State {
	state := sqlgen.NewState()
	state.ReplaceRule(sqlgen.ColumnDefinitionType,
		sqlgen.Or(
			sqlgen.ColumnDefinitionTypesStrings,
			sqlgen.ColumnDefinitionTypesIntegerInt,
		),
	)
	// TiDB does not support LIST partition by default.
	state.SetWeight(sqlgen.PartitionDefinitionList, 0)
	return state
}
