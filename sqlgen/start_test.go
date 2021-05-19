package sqlgen_test

import (
	"fmt"
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
		res := sqlgen.Start.Eval(state)
		c.Assert(len(res) > 0, IsTrue, Commentf(state.LastBrokenAssumption()))
		c.Assert(len(res), Greater, 0, Commentf("i = %d", i))
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
		c.Assert(len(res) > 0, IsTrue, Commentf(state.LastBrokenAssumption()))
		intColCount += strings.Count(res, "int")
	}
	c.Assert(intColCount, Equals, 100*5)
}

func (s *testSuite) TestPredicates(c *C) {
	state := sqlgen.NewState()
	state.SetRepeat(sqlgen.ColumnDefinition, 10, 10)
	_ = sqlgen.CreateTable.Eval(state)
	state.Store(sqlgen.ScopeKeyCurrentTables, sqlgen.Tables(state.GetAllTables()))
	defer state.DestroyScope()
	for i := 0; i < 100; i++ {
		pred := sqlgen.Predicates.Eval(state)
		fmt.Println(pred)
		if strings.Contains(pred, "or") {
			c.Assert(strings.Contains(pred, " or "), IsTrue, Commentf(pred))
		}
		if strings.Contains(pred, "and") {
			c.Assert(strings.Contains(pred, " and "), IsTrue, Commentf(pred))
		}
	}
}
