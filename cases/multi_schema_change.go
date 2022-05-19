package cases

import "github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"

func NewMultiSchemaChangeState() *sqlgen.State {
	state := NewStateForTiDB600()
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	state.SetWeight(sqlgen.AlterTableChangeMulti, 2)
	// TiDB implementation of float and double is different from MySQL.
	state.SetWeight(sqlgen.ColumnDefinitionTypesFloat, 0)
	state.SetWeight(sqlgen.ColumnDefinitionTypesDouble, 0)
	state.SetWeight(sqlgen.ColumnDefinitionTypesJSON, 0)
	// We only care about the correctness of data after multi-schema change.
	state.ReplaceRule(sqlgen.Query, sqlgen.QueryAll)
	// Increase the alter table weight.
	state.SetWeight(sqlgen.AlterTable, 15)
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 10)
	state.SetRepeat(sqlgen.IndexDefinition, 3, 7)
	return state
}
