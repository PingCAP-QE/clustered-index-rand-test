package sqlgen

type Hooks struct {
	hooks []FnEvaluateHook
}

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

func (h *Hooks) Find(name string) FnEvaluateHook {
	for _, h := range h.hooks {
		if h.Info() == name {
			return h
		}
	}
	return nil
}

func (h *Hooks) Append(hook FnEvaluateHook) {
	h.hooks = append(h.hooks, hook)
}

func (h *Hooks) Remove(hookInfo string) {
	idx := 0
	for i, h := range h.hooks {
		if h.Info() == hookInfo {
			idx = i
		}
	}
	h.hooks = append(h.hooks[:idx], h.hooks[idx+1:]...)
}
