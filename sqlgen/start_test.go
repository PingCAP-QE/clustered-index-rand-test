package sqlgen

import (
	"math/rand"
	"testing"
)

func TestInitialStart(t *testing.T) {
	state := NewState()
	rand.Seed(10086)
	for i := 0; i < 200; i++ {
		start.Eval(state)
	}
}
