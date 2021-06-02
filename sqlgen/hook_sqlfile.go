package sqlgen

import "os"

var _ FnEvaluateHook = (*FnHookSQLFile)(nil)

type FnHookSQLFile struct {
	FnHookDefault
	file os.File
}

func (s *FnHookSQLFile) BeforeEvaluate(state *State, fn Fn) Fn {
	return fn
}

func (s *FnHookSQLFile) AfterEvaluate(state *State, fn Fn, result string) string {
	return result
}

func NewFnHookSQLFile(state *State) *FnHookSQLFile {
	return &FnHookSQLFile{
		FnHookDefault: NewFnHookDefault("scope"),
	}
}
