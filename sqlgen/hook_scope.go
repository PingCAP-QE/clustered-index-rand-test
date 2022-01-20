package sqlgen

var _ FnEvaluateHook = (*FnHookScope)(nil)

type FnHookScope struct {
	FnHookDefault
}

func (s *FnHookScope) BeforeEvaluate(state *State, fn Fn) Fn {
	state.env.Enter()
	state.env.FnInfo = fn.Info
	state.fnStack = state.env.GetCurrentStack()
	return fn
}

func (s *FnHookScope) AfterEvaluate(state *State, fn Fn, result string) string {
	state.env.Leave()
	state.CheckIntegrity()
	return result
}

func NewFnHookScope(state *State) *FnHookScope {
	return &FnHookScope{
		FnHookDefault: NewFnHookDefault("scope"),
	}
}
