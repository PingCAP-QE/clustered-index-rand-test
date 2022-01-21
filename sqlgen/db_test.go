package sqlgen_test

import (
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	"github.com/stretchr/testify/require"
)

func TestRandGBKStrings(t *testing.T) {
	res := sqlgen.RandGBKStringRunes(10)
	require.Greater(t, len(res), 10, res)
}
