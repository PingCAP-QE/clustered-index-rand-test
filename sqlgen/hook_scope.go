package sqlgen

var _ FnEvaluateHook = (*FnHookScope)(nil)

type FnHookScope struct {
	FnHookDefault
	state *State
}

func (s *FnHookScope) BeforeEvaluate(fn Fn) Fn {
	s.state.CreateScope()
	return fn
}

func (s *FnHookScope) AfterEvaluate(fn Fn, result string) string {
	s.state.DestroyScope()
	return result
}

func NewFnHookScope(state *State) *FnHookScope {
	return &FnHookScope{
		FnHookDefault: NewFnHookDefault("scope"),
		state:         state,
	}
}