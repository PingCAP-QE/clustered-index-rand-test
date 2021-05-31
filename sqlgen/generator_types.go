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
	"runtime"
)

type Fn struct {
	Gen    func(state *State) string
	Info   string
	Weight int
	Repeat Interval
}

func defaultFn() Fn {
	return Fn{
		Weight: 1,
		Repeat: Interval{1, 3},
	}
}

func NewFn(fn func(state *State) Fn) Fn {
	_, filePath, line, _ := runtime.Caller(1)
	ret := defaultFn()
	ret.Info = constructFnInfo(filePath, line)
	ret.Gen = func(state *State) string {
		return fn(state).Eval(state)
	}
	return ret
}

func (f Fn) Equal(other Fn) bool {
	if f.Info == "" || other.Info == "" {
		return false
	}
	return f.Info == other.Info
}

func (f Fn) SetW(weight int) Fn {
	newFn := f
	newFn.Weight = weight
	return newFn
}

func (f Fn) SetR(low, high int) Fn {
	newFn := f
	newFn.Repeat = Interval{lower: low, upper: high}
	return newFn
}

func (f Fn) SetRI(fixed int) Fn {
	newFn := f
	newFn.Repeat = Interval{lower: fixed, upper: fixed}
	return newFn
}

func (f Fn) Eval(state *State) string {
	newFn := f
	for _, l := range state.hooks {
		newFn = l.BeforeEvaluate(state, newFn)
	}
	var res string
	if state.GetWeight(f) == 0 {
		res = ""
	} else {
		res = newFn.Gen(state)
	}
	for _, l := range state.hooks {
		res = l.AfterEvaluate(state, newFn, res)
	}
	return res
}
