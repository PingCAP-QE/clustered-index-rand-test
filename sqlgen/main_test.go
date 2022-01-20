package sqlgen

import (
	"math/rand"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	rand.Seed(10086)
	exitCode := m.Run()
	os.Exit(exitCode)
}
