package sqlgen

var _ FnEvaluateHook = (*FnHookReplacer)(nil)

const HookNameReplacer = "replace"

type FnHookReplacer struct {
	FnHookDefault
	dict map[string]Fn
}

func (h *FnHookReplacer) Replace(targetFn Fn, newFn Fn) {
	h.dict[targetFn.Info] = newFn
}

func (h *FnHookReplacer) RemoveReplace(targetFn Fn) {
	delete(h.dict, targetFn.Info)
}

func (h *FnHookReplacer) BeforeEvaluate(state *State, fn Fn) Fn {
	if newFn, ok := h.dict[fn.Info]; ok {
		if newFn.Info == fn.Info {
			// Prevent replace itself.
			return fn
		}
		return newFn
	}
	return fn
}

func NewFnHookReplacer() *FnHookReplacer {
	return &FnHookReplacer{
		FnHookDefault: NewFnHookDefault(HookNameReplacer),
		dict:          map[string]Fn{},
	}
}
