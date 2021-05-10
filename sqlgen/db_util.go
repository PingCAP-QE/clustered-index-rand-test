package sqlgen

import (
	"github.com/davecgh/go-spew/spew"
	"log"
	"runtime/debug"
)

func Assert(cond bool, targets ...interface{}) {
	if !cond {
		spew.Dump(targets...)
		debug.PrintStack()
		log.Fatal("assertion failed")
	}
}

func NeverReach() Fn {
	debug.PrintStack()
	log.Fatal("assertion failed: should not reach here")
	return Fn{}
}
