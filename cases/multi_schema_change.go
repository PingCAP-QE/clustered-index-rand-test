package cases

import "github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"

func NewMultiSchemaChangeState() *sqlgen.State {
	state := NewStateForTiDB600()
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	state.SetWeight(sqlgen.AlterTableChangeMulti, 2)
	return state
}
