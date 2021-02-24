package sqlgen

import (
	"github.com/davecgh/go-spew/spew"
	"log"
)

func Assert(cond bool, targets ...interface{}) {
	if !cond {
		spew.Dump(targets...)
		log.Fatal("assertion failed")
	}
}
