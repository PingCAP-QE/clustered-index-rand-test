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
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

func And(fns ...Fn) Fn {
	ret := defaultFn()
	ret.Info = "And"
	ret.Gen = func(state *State) string {
		var resStr strings.Builder
		for i, f := range fns {
			Assert(state.GetPrerequisite(f)(state))
			if i != 0 {
				resStr.WriteString(" ")
			}
			res := f.Eval(state)
			resStr.WriteString(strings.Trim(res, " "))
		}
		return resStr.String()
	}
	return ret
}

func Or(fns ...Fn) Fn {
	ret := defaultFn()
	ret.Info = "Or"
	ret.Gen = func(state *State) string {
		Assert(len(fns) > 0)
		chosenFnIdx := randSelectByWeight(state, fns)
		chosenFn := fns[chosenFnIdx]
		return chosenFn.Eval(state)
	}
	return ret
}

// Str is a Fn which simply returns str.
func Str(str string) Fn {
	ret := defaultFn()
	ret.Info = "Str"
	ret.Gen = func(_ *State) string {
		return str
	}
	return ret
}

func Strf(str string, fns ...Fn) Fn {
	if len(fns) == 0 {
		return Str(str)
	}
	ss := strings.Split(str, "[%fn]")
	if len(ss) != len(fns)+1 {
		panic(fmt.Sprintf("[param count mismatched] str: %s", str))
	}
	strs := make([]Fn, 0, 2*len(ss)-1)
	for i := 0; i < len(fns); i++ {
		strs = append(strs, Str(ss[i]))
		strs = append(strs, fns[i])
		if i == len(fns)-1 {
			strs = append(strs, Str(ss[i+1]))
		}
	}
	return And(strs...)
}

func Strs(strs ...string) Fn {
	ret := defaultFn()
	ret.Info = "Strs"
	ret.Gen = func(state *State) string {
		return strings.Join(strs, " ")
	}
	return ret
}

func If(condition bool, fn Fn) Fn {
	if condition {
		return fn
	}
	return Empty
}

func Repeat(fn Fn, sep Fn) Fn {
	ret := defaultFn()
	ret.Info = "Repeat"
	ret.Gen = func(state *State) string {
		var resStr strings.Builder
		count := randGenRepeatCount(state, fn)
		for i := 0; i < count; i++ {
			if !state.GetPrerequisite(fn)(state) {
				break
			}
			res := fn.Eval(state)
			s := strings.Trim(res, " \n\t")
			if len(s) == 0 {
				continue
			}
			if i != 0 {
				sepRes := sep.Eval(state)
				resStr.WriteString(" ")
				resStr.WriteString(sepRes)
			}
			resStr.WriteString(s)
		}
		return resStr.String()
	}
	return ret
}

func RepeatCount(fn Fn, cnt int, sep Fn) Fn {
	if cnt == 0 {
		return Empty
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

func Join(fns []Fn, sep Fn) Fn {
	ret := make([]Fn, 0, 2*len(fns)-1)
	for i, f := range fns {
		if i != 0 {
			ret = append(ret, sep)
		}
		ret = append(ret, f)
	}
	return And(ret...)
}

var Empty = NewFn(func(state *State) Fn {
	return Str("")
})

var None = NewFn(func(state *State) Fn {
	return Str("")
})

func Opt(fn Fn) Fn {
	ret := defaultFn()
	ret.Gen = func(state *State) string {
		total := 1 + state.GetWeight(fn)
		if rand.Intn(total) == 0 {
			return ""
		}
		return fn.Eval(state)
	}
	return ret
}

func Num(v int) string {
	return strconv.FormatInt(int64(v), 10)
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

func ShouldValid(i int) bool {
	return rand.Intn(100) < i
}
