package sqlgen

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	state := NewState()
	DefaultWeight.Query_INDEX_MERGE = true
	DefaultWeight.Query = 100
	DefaultWeight.Query_DML = 0
	DefaultWeight.Query_DDL = 0
	DefaultWeight.CreateTable_IndexMoreCol = 5
	state.WithWeight(&DefaultWeight)
	state.InjectTodoSQL("set @@tidb_enable_clustered_index=true")
	gen := NewGenerator(state)
	for i := 0; i < 200; i++ {
		fmt.Printf("%s;\n", gen())
	}
}
