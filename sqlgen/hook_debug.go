package sqlgen

var _ FnEvaluateHook = (*FnHookDebug)(nil)

type FnHookDebug struct {
	FnHookDefault
	parentsFn      []string
	obtainedResult []string
}

func (d *FnHookDebug) BeforeEvaluate(fn Fn) Fn {
	d.parentsFn = append(d.parentsFn, fn.Info)
	return fn
}

func (d *FnHookDebug) AfterEvaluate(fn Fn, result string) string {
	d.parentsFn = d.parentsFn[:len(d.parentsFn)-1]
	d.obtainedResult = append(d.obtainedResult, result)
	return result
}

func NewFnHookDebug() *FnHookDebug {
	return &FnHookDebug{FnHookDefault: NewFnHookDefault("debug")}
}
