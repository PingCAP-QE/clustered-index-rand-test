// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlgen

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

type ProductionListener interface {
	BeforeProductionGen(fn Fn) Fn
	AfterProductionGen(fn Fn, res string) string
}

func And(fn ...Fn) Fn {
	return Fn{Weight: 1, Gen: func(state *State) string {
		return collectResult(state, fn...)
	}}
}

func Opt(fn Fn) Fn {
	if RandomBool() {
		return fn
	}
	return Empty()
}

func RandomNum(low, high int64) string {
	num := rand.Int63n(high - low + 1)
	return strconv.FormatInt(num+low, 10)
}

func RandomNums(low, high int64, count int) []string {
	nums := make([]int64, count)
	for i := 0; i < count; i++ {
		nums[i] = low + rand.Int63n(high-low+1)
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = strconv.FormatInt(nums[i], 10)
	}
	return result
}

func RandomFloat(low, high float64) float64 {
	return low + rand.Float64()*(high-low)
}

func RandomBool() bool {
	return rand.Intn(2) == 0
}

func If(condition bool, fn Fn) Fn {
	if condition {
		return fn
	}
	return NoneFn()
}

func OptIf(condition bool, fn Fn) Fn {
	if condition {
		return fn
	}
	return Empty()
}

func Or(fns ...Fn) Fn {
	fns = filterNoneFns(fns)
	return Fn{Weight: 1, Gen: func(state *State) string {
		randNum := randomSelectByFactor(fns, func(f Fn) int {
			return f.Weight
		})
		chosenFn := fns[randNum]
		return chosenFn.Eval(state)
	}}
}

func Repeat(fn Fn, cnt int, sep Fn) Fn {
	if cnt == 0 {
		return Empty()
	}
	fns := make([]Fn, 0, 2*cnt-1)
	for i := 0; i < cnt; i++ {
		fns = append(fns, fn)
		if i != cnt-1 {
			fns = append(fns, sep)
		}
	}
	return And(fns...)
}

func RepeatRange(low, high int, fn Fn, sep Fn) Fn {
	return Repeat(fn, low+rand.Intn(high+1-low), sep)
}

func Join(sep Fn, fns ...Fn) Fn {
	newFns := make([]Fn, 0, len(fns)*2-1)
	for i, f := range fns {
		newFns = append(newFns, f)
		if i != len(fns)-1 {
			newFns = append(newFns, sep)
		}
	}
	return And(newFns...)
}

func filterNoneFns(fns []Fn) []Fn {
	for i := 0; i < len(fns); i++ {
		isNoneFn := fns[i].Gen == nil
		if isNoneFn {
			fns[i], fns[len(fns)-1] = fns[len(fns)-1], fns[i]
			fns = fns[:len(fns)-1]
			i--
		}
	}
	return fns
}

func collectResult(state *State, fns ...Fn) string {
	var doneF []Fn
	var resStr strings.Builder
	for i, f := range fns {
		res := f.Eval(state)
		doneF = append(doneF, f)
		resStr.WriteString(strings.Trim(res, " "))
		if i != len(fns) {
			resStr.WriteString(" ")
		}
	}
	return resStr.String()
}

func randomSelectByFactor(fns []Fn, weightFn func(f Fn) int) int {
	num := rand.Intn(sumRandFactor(fns, weightFn))
	acc := 0
	for i, f := range fns {
		acc += weightFn(f)
		if acc > num {
			return i
		}
	}
	return len(fns) - 1
}

func sumRandFactor(fs []Fn, weightFn func(f Fn) int) int {
	total := 0
	for _, f := range fs {
		total += weightFn(f)
	}
	return total
}
