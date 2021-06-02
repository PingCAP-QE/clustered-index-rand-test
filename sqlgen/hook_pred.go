package sqlgen

var _ FnEvaluateHook = (*FnHookPred)(nil)

type FnHookPred struct {
	FnHookDefault
	toMatchFns []Fn
	matched    bool
}

func (d *FnHookPred) BeforeEvaluate(state *State, fn Fn) Fn {
	return fn
}

func (d *FnHookPred) AfterEvaluate(state *State, fn Fn, result string) string {
	if state.IsValid() {
		return result
	}
	for _, mf := range d.toMatchFns {
		if mf.Equal(fn) {
			d.matched = true
			break
		}
	}
	return result
}

func (d *FnHookPred) ResetMatched() {
	d.matched = false
}

func (d *FnHookPred) Matched() bool {
	return d.matched
}

func (d *FnHookPred) AddMatchFn(fn Fn) {
	d.toMatchFns = append(d.toMatchFns, fn)
}

func (d *FnHookPred) Build(fns []Fn) *FnHookPred {
	for _, f := range fns {
		d.AddMatchFn(f)
	}
	return d
}

func NewFnHookPred() *FnHookPred {
	return &FnHookPred{FnHookDefault: NewFnHookDefault("debug")}
}
