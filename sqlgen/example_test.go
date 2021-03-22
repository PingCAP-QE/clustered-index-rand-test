package sqlgen

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	state := NewState()
	state.WithWeight(&DefaultWeight)
	state.InjectTodoSQL("set global tidb_enable_clustered_index=true")
	gen := NewGenerator(state)
	for i := 0; i < 200; i++ {
		fmt.Printf("%s;\n", gen())
	}
}
