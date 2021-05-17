package sqlgen_test

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct{}

func (s *testSuite) SetUpSuite(c *C) {
	rand.Seed(10086)
}

func (s *testSuite) TestStart(c *C) {
	state := sqlgen.NewState()
	for i := 0; i < 300; i++ {
		sqlgen.Start.Eval(state)
		c.Assert(state.Valid(), IsTrue, Commentf(state.LastBrokenAssumption()))
	}
}

func (s *testSuite) TestCreateColumnTypes(c *C) {
	state := sqlgen.NewState()
	state.StoreConfig(sqlgen.ConfigKeyIntMaxTableCount, 100)
	state.StoreConfig(sqlgen.ConfigKeyArrayAllowColumnTypes, []sqlgen.ColumnType{sqlgen.ColumnTypeInt})
	state.SetRepeat(sqlgen.ColumnDefinition, 5, 5)
	intColCount := 0
	for i := 0; i < 100; i++ {
		res := sqlgen.CreateTable.Eval(state)
		c.Assert(state.Valid(), IsTrue, Commentf(state.LastBrokenAssumption()))
		intColCount += strings.Count(res, "int")
	}
	c.Assert(intColCount, Equals, 100*5)
}
