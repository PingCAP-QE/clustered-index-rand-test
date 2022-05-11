package cases

import (
	. "github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
)

func NewStateForTiDB600() *State {
	state := NewState()
	// Disable flashback table for unistore.
	state.SetWeight(FlashBackTable, 0)
	// TiDB does not support LIST partition by default.
	state.SetWeight(PartitionDefinitionList, 0)
	return state
}
