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
	"runtime"
)

var copyID int64

type Fn struct {
	Gen          func(state *State) (string, error)
	Info         string
	Weight       int
	Repeat       Interval
	Prerequisite func(state *State) bool
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
	ret.Gen = func(state *State) (string, error) {
		return fn(state).Eval(state)
	}
	return ret
}

func (f Fn) Copy() Fn {
	copyID++
	f.Info = fmt.Sprintf("%s%d", f.Info, copyID)
	return f
}

func (f Fn) Equal(other Fn) bool {
	if f.Info == "" || other.Info == "" {
		return false
	}
	return f.Info == other.Info
}

func (f Fn) W(weight int) Fn {
	newFn := f
	newFn.Weight = weight
	return newFn
}

func (f Fn) R(low, high int) Fn {
	newFn := f
	newFn.Repeat = Interval{lower: low, upper: high}
	return newFn
}

func (f Fn) P(fns ...func(state *State) bool) Fn {
	newFn := f
	newFn.Prerequisite = func(state *State) bool {
		for _, pre := range fns {
			if pre == nil {
				continue
			}
			if !pre(state) {
				return false
			}
		}
		return true
	}
	return newFn
}

func (f Fn) Eval(state *State) (res string, err error) {
	newFn := f
	for _, l := range state.hooks.hooks {
		newFn = l.BeforeEvaluate(state, newFn)
	}
	if state.GetWeight(f) != 0 {
		res, err = newFn.Gen(state)
	}
	for _, l := range state.hooks.hooks {
		res = l.AfterEvaluate(state, newFn, res)
	}
	return res, err
}
