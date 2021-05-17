package sqlgen

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestStart(t *testing.T) {
	state := NewState()
	rand.Seed(10086)
	for i := 0; i < 300; i++ {
		fmt.Println(Start.Eval(state))
	}
}

func TestCreateColumnTypes(t *testing.T) {
	state := NewState()
	state.ctrl.MaxTableNum = 100
	state.StoreConfig(ConfigKeyArrayAllowColumnTypes, NewScopeObj([]ColumnType{ColumnTypeInt}))
	state.SetRepeat(ColumnDefinition, 5, 5)
	rand.Seed(10086)
	intColCount := 0
	for i := 0; i < 100; i++ {
		res := CreateTable.Eval(state)
		if !state.Valid() {
			t.Fatal(state.LastBrokenAssumption())
		}
		intColCount += strings.Count(res, "int")
	}
	if intColCount != 100*5 {
		t.Fatal()
	}
}
