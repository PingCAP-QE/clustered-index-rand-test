package sqlgen

var _ FnEvaluateHook = (*FnHookScope)(nil)

type FnHookScope struct {
	FnHookDefault
}

func (s *FnHookScope) BeforeEvaluate(state *State, fn Fn) Fn {
	state.CreateScope()
	state.Store(ScopeKeyCurrentFn, fn.Info)
	state.fnStack = state.GetCurrentStack()
	return fn
}

func (s *FnHookScope) AfterEvaluate(state *State, fn Fn, result string) string {
	state.DestroyScope()
	return result
}

func NewFnHookScope(state *State) *FnHookScope {
	return &FnHookScope{
		FnHookDefault: NewFnHookDefault("scope"),
	}
}
