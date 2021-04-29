package sqlgen

import (
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	state := NewState()
	state.InjectTodoSQL("set global tidb_enable_clustered_index=true")
	gen := NewGenerator(state)
	for i := 0; i < 200; i++ {
		fmt.Printf("%s;\n", gen())
	}
}

func TestB(t *testing.T) {
	state := NewState(func(ctl *ControlOption) {
		ctl.Weight.MustCTE = true
		ctl.Weight.CreateTable_MustIntCol = true
		ctl.InitTableCount = 2
		ctl.Weight.CTEValidSQL = 100
	})
	gen := NewGenerator(state)
	for i := 0; i < 200; i++ {
		fmt.Printf("%s;\n", gen())
	}
}

func TestC(t *testing.T) {
	state := NewState(func(ctl *ControlOption) {
		ctl.Weight.CTEJustSyntax = true
	})
	gen := NewGenerator(state)
	for i := 0; i < 200; i++ {
		s := gen()
		fmt.Printf("\"%s;\",\n", s)
	}
}
