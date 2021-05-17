package sqlgen

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
)

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
		} else {
			return result
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

func randGenRepeatCount(state *State, fn Fn) int {
	low, high := fn.Repeat.lower, fn.Repeat.upper
	if l, h, ok := state.GetRepeat(fn); ok {
		low, high = l, h
	}
	return low + rand.Intn(high+1-low)
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
