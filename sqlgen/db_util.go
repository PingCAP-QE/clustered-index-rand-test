package sqlgen

import (
	"log"
	"math/rand"
	"runtime/debug"

	"github.com/davecgh/go-spew/spew"
)

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

type Entity interface {
	*Table | *Column | *Index | *TableColumns
}

func gRand1[T Entity](is []T) T {
	if len(is) == 0 {
		return nil
	}
	return is[rand.Intn(len(is))]
}

func gRandN[T Entity](is []T, n int) []T {
	newIs := gCopy(is)
	rand.Shuffle(len(newIs), func(i, j int) {
		newIs[i], newIs[j] = newIs[j], newIs[i]
	})
	return newIs[:n]
}

func gFilter[T Entity](is []T, pred func(T) bool) []T {
	ret := make([]T, 0, len(is)/2)
	for _, t := range is {
		if pred(t) {
			ret = append(ret, t)
		}
	}
	return ret
}

func gExist[T Entity](is []T, pred func(T) bool) bool {
	for _, t := range is {
		if pred(t) {
			return true
		}
	}
	return false
}

func gContain[T Entity](is []T, other T) bool {
	for _, t := range is {
		if other == t {
			return true
		}
	}
	return false
}

func gDiff[T Entity](src, dest []T) []T {
	ret := make([]T, 0, len(src)/2)
	for _, i := range src {
		if !gContain(dest, i) {
			ret = append(ret, i)
		}
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

func gCopy[T Entity](is []T) []T {
	newIs := make([]T, len(is))
	for i := 0; i < len(is); i++ {
		newIs[i] = is[i]
	}
	return newIs
}

func gConcat[T Entity](iss ...[]T) []T {
	newIs := make([]T, 0)
	for _, is := range iss {
		for _, i := range is {
			newIs = append(newIs, i)
		}
	}
	return newIs
}

func gSpan[T Entity](is []T, pred func(T) bool) ([]T, []T) {
	match, unmatch := make([]T, 0), make([]T, 0)
	for _, t := range is {
		if pred(t) {
			match = append(match, t)
		} else {
			unmatch = append(unmatch, t)
		}
	}
	return match, unmatch
}

func gMap[T Entity](is []T, fn func(T) T) []T {
	newIs := gCopy(is)
	for i, t := range newIs {
		newIs[i] = fn(t)
	}
	return newIs
}

func gFold[T Entity](is []T, init T, fn func(a, b T) T) T {
	total := init
	for _, i := range is {
		total = fn(total, i)
	}
	return total
}
