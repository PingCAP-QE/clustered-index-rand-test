package sqlgen

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	state := NewState()
	state.ctrl.Weight.Query_Select = 1000
	state.ctrl.Weight.Query_DDL = 0
	state.ctrl.Weight.Query_DML = 0
	state.InjectTodoSQL("set global tidb_enable_clustered_index=true")
	gen := NewGenerator(state)
	for i := 0; i < 200; i++ {
		fmt.Printf("%s;\n", gen())
	}
}
