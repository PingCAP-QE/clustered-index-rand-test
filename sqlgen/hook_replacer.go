package sqlgen

var _ FnEvaluateHook = (*FnHookReplacer)(nil)

type FnHookReplacer struct {
	FnHookDefault
	dict map[string]Fn
}

func (h *FnHookReplacer) Replace(targetFn Fn, newFn Fn) {
	h.dict[targetFn.Info] = newFn
}

func (h *FnHookReplacer) BeforeEvaluate(state *State, fn Fn) Fn {
	if newFn, ok := h.dict[fn.Info]; ok {
		return newFn
	}
	return fn
}

func NewFnHookReplacer() *FnHookReplacer {
	return &FnHookReplacer{
		FnHookDefault: NewFnHookDefault("replacer"),
		dict:          map[string]Fn{},
	}
}
