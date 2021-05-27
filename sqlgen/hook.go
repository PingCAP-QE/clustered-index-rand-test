package sqlgen

type FnEvaluateHook interface {
	Info() string
	BeforeEvaluate(state *State, fn Fn) Fn
	AfterEvaluate(state *State, fn Fn, res string) string
}

var _ FnEvaluateHook = (*FnHookDefault)(nil)

type FnHookDefault struct {
	info string
}

func (s FnHookDefault) BeforeEvaluate(state *State, fn Fn) Fn {
	return fn
}

func (s FnHookDefault) AfterEvaluate(state *State, _ Fn, res string) string {
	return res
}

func (s FnHookDefault) Info() string {
	return s.info
}

func NewFnHookDefault(info string) FnHookDefault {
	return FnHookDefault{info: info}
}
