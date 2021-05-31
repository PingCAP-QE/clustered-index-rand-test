package sqlgen

import "fmt"

var _ FnEvaluateHook = (*FnHookDebug)(nil)

type FnHookDebug struct {
	FnHookDefault
	parentFn []string
}

func (d *FnHookDebug) BeforeEvaluate(state *State, fn Fn) Fn {
	d.parentFn = append(d.parentFn, fn.Info)
	fmt.Printf("evaluating %v\n", d.parentFn)
	return fn
}

func (d *FnHookDebug) AfterEvaluate(state *State, fn Fn, result string) string {
	d.parentFn = d.parentFn[:len(d.parentFn)-1]
	return result
}

func NewFnHookDebug() *FnHookDebug {
	return &FnHookDebug{FnHookDefault: NewFnHookDefault("debug")}
}
