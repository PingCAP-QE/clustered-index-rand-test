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

func NeverReach() Fn {
	log.Fatal("assertion failed: should not reach here")
	return Fn{}
}
