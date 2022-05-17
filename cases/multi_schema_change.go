package cases

import "github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"

var QueryForMultiSchemaChange = sqlgen.NewFn(func(state *sqlgen.State) sqlgen.Fn {
	if len(state.Tables) == 0 {
		return sqlgen.None("no tables")
	}
	tbl := state.Tables.Rand()
	return sqlgen.Strs("select * from", tbl.Name)
})

func NewMultiSchemaChangeState() *sqlgen.State {
	state := NewStateForTiDB600()
	state.SetWeight(sqlgen.PartitionDefinition, 0)
	state.SetWeight(sqlgen.AlterTableChangeMulti, 2)
	// TiDB implementation of float and double is different from MySQL.
	state.SetWeight(sqlgen.ColumnDefinitionTypesFloat, 0)
	state.SetWeight(sqlgen.ColumnDefinitionTypesDouble, 0)
	state.SetWeight(sqlgen.ColumnDefinitionTypesJSON, 0)
	state.ReplaceRule(sqlgen.Query, QueryForMultiSchemaChange)
	state.SetWeight(sqlgen.AlterTable, 15)
	return state
}
