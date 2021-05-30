package sqlgen

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"log"
	"runtime/debug"
)

type TableColumnPair struct {
	t *Table
	c *Column
}

type TableColumnPairs []TableColumnPair

func NewTableColumnPairs1ToN(t *Table, cols []*Column) TableColumnPairs {
	ret := make([]TableColumnPair, 0, len(cols))
	for _, c := range cols {
		ret = append(ret, TableColumnPair{t: t, c: c})
	}
	return ret
}

func NewTableColumnPairsNToN(tbs []*Table, cols []*Column) TableColumnPairs {
	Assert(len(tbs) == len(cols))
	ret := make([]TableColumnPair, 0, len(cols))
	for i, c := range cols {
		ret = append(ret, TableColumnPair{t: tbs[i], c: c})
	}
	return ret
}

type Interval struct {
	lower int
	upper int
}

func Assert(cond bool, targets ...interface{}) {
	if !cond {
		spew.Dump(targets...)
		debug.PrintStack()
		log.Fatal("assertion failed")
	}
}

type Columns []*Column

func NeverReach(msgs ...string) Fn {
	debug.PrintStack()
	if len(msgs) > 0 {
		errMsg := fmt.Sprintf("assertion failed: should not reach here %s", msgs)
		log.Fatal(errMsg)
	}
	log.Fatal("assertion failed: should not reach here")
	return defaultFn()
}
