package sqlgen_test

import (
	"github.com/PingCAP-QE/clustered-index-rand-test/sqlgen"
	. "github.com/pingcap/check"
)

func (s testSuite) TestMove(c *C) {
	var result []int
	get := func(i int) interface{} { return result[i] }
	set := func(i int, v interface{}) { result[i] = v.(int) }
	testMove := func(src, dest int, expected []int) {
		result = []int{1, 2, 3, 4, 5}
		sqlgen.Move(src, dest, get, set)
		c.Assert(result, DeepEquals, expected, Commentf("src: %d, dest: %d", src, dest))
	}
	testMove(2, 0, []int{3, 1, 2, 4, 5})
	testMove(3, 1, []int{1, 4, 2, 3, 5})
	testMove(4, 3, []int{1, 2, 3, 5, 4})
	testMove(4, 0, []int{5, 1, 2, 3, 4})

	testMove(0, 2, []int{2, 3, 1, 4, 5})
	testMove(2, 3, []int{1, 2, 4, 3, 5})
	testMove(2, 4, []int{1, 2, 4, 5, 3})
	testMove(0, 4, []int{2, 3, 4, 5, 1})

	testMove(0, 0, []int{1, 2, 3, 4, 5})
	testMove(2, 2, []int{1, 2, 3, 4, 5})
	testMove(4, 4, []int{1, 2, 3, 4, 5})
}
