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
	"bufio"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
)

type Fn struct {
	Gen    func(state *State) string
	Info   string
	Weight int
}

// NewFn can only be used for
func NewFn(fn func(state *State) Fn) Fn {
	_, filePath, line, _ := runtime.Caller(1)
	return Fn{
		Info: constructFnInfo(filePath, line),
		Gen: func(state *State) string {
			return fn(state).Eval(state)
		},
		Weight: 1,
	}
}

func constructFnInfo(filePath string, line int) string {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	Assert(err == nil)
	sc := bufio.NewScanner(file)
	currentLine := 0
	for sc.Scan() {
		currentLine++
		if currentLine != line {
			continue
		}
		result := extractVarName(sc.Text())
		if result == "" {
			return fmt.Sprintf("%s-%d", filePath, line)
		}
	}
	return ""
}

var newFnUsagePattern = regexp.MustCompile("(?P<VAR>(var)?).*(?P<FN>(:?)=\\s*NewFn)")

func extractVarName(source string) string {
	locs := newFnUsagePattern.FindStringSubmatchIndex(source)
	ns := newFnUsagePattern.SubexpNames()
	var varSymEnd, assignSymBegin int
	if len(locs) > 0 {
		for i, n := range ns {
			if n == "VAR" {
				varSymEnd = locs[i*2+1]
			}
			if n == "FN" {
				assignSymBegin = locs[i*2]
			}
		}
	}
	if assignSymBegin != 0 {
		ret := strings.Trim(source[varSymEnd:assignSymBegin], " ")
		return ret
	}
	return ""
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

func (f Fn) Eval(state *State) string {
	newFn := f
	for _, l := range state.hooks {
		newFn = l.BeforeEvaluate(newFn)
	}
	res := newFn.Gen(state)
	for _, l := range state.hooks {
		res = l.AfterEvaluate(newFn, res)
	}
	return res
}

// Str is a Fn which simply returns str.
func Str(str string) Fn {
	return Fn{
		Weight: 1,
		Gen: func(_ *State) string {
			return str
		}}
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
	return Fn{
		Weight: 1,
		Gen: func(state *State) string {
			return strings.Join(strs, " ")
		},
	}
}

// Empty is a Fn which simply returns empty string.
func Empty() Fn {
	return innerEmptyFn
}

var innerEmptyFn = Fn{
	Weight: 1,
	Gen: func(state *State) string {
		return ""
	},
}

func None() Fn {
	return innerNoneFn
}

var innerNoneFn = Str("")
