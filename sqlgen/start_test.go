package sqlgen

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestStart(t *testing.T) {
	state := NewState()
	rand.Seed(10086)
	for i := 0; i < 300; i++ {
		fmt.Println(Start.Eval(state))
	}
}
