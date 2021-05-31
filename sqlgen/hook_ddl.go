package sqlgen

var _ FnEvaluateHook = (*FnHookHint)(nil)

type FnHookHint struct {
	FnHookDefault
}

func (d *FnHookHint) BeforeEvaluate(state *State, fn Fn) Fn {
	return fn
}

func (d *FnHookHint) AfterEvaluate(state *State, fn Fn, result string) string {
	if len(fn.Hint) != 0 {
		return fn.Hint + " " + result
	}
	return result
}

func NewFnHookHint(state *State) *FnHookHint {
	return &FnHookHint{
		FnHookDefault: NewFnHookDefault("hint"),
	}
}

