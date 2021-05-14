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

func And(fn ...Fn) Fn {
	return Fn{Weight: 1, Gen: func(state *State) string {
		return collectResult(state, fn...)
	}}
}

func Opt(fn Fn) Fn {
	total := 1 + fn.Weight
	if rand.Intn(total) == 0 {
		return Empty()
	}
	return fn
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

func OptIf(condition bool, fn Fn) Fn {
	if condition {
		return fn
	}
	return Empty()
}

func Or(fns ...Fn) Fn {
	return Fn{Weight: 1, Gen: func(state *State) string {
		for len(fns) > 0 {
			chosenFnIdx := randSelectByWeight(state, fns)
			chosenFn := fns[chosenFnIdx]
			result := chosenFn.Eval(state)
			if state.Valid() {
				return result
			}
			state.Recover()
			fns = append(fns[:chosenFnIdx], fns[chosenFnIdx+1:]...)
		}
		// Need backtracking.
		state.Invalidate()
		return ""
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

func randSelectByWeight(state *State, fns []Fn) int {
	totalWeight := 0
	for _, f := range fns {
		totalWeight += state.GetWeight(f)
	}
	num := rand.Intn(totalWeight)
	acc := 0
	for i, f := range fns {
		acc += f.Weight
		if acc > num {
			return i
		}
	}
	return len(fns) - 1
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
