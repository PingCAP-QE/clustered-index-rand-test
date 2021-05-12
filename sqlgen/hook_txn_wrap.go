package sqlgen

var _ FnEvaluateHook = (*FnHookTxnWrap)(nil)

type FnHookTxnWrap struct {
	FnHookDefault
	inTxn         bool
	inflightStmts int
	ctl           *ControlOption
}

const txnStartWrapName = "txnWrappedStart"

func (s *FnHookTxnWrap) BeforeEvaluate(fn Fn) Fn {
	if fn.Info == start.Info {
		return fn
	}
	return Fn{
		Info: txnStartWrapName,
		Gen: func(state *State) string {
			startTxnRs := s.startTxn()
			currRs := fn.Gen(state)
			return startTxnRs + " ; " + currRs
		},
	}
}

func (s *FnHookTxnWrap) startTxn() string {
	fns := []Fn{
		Empty().SetW(1),
		And(
			Str("begin"),
			Or(
				Str("pessimistic"),
				Str("optimistic"),
			),
		).SetW(1),
	}
	var chosenFn Fn
	if s.inTxn {
		// inside txn, never begin again.
		chosenFn = fns[0]
	} else {
		chosenFn = fns[randomSelectByFactor(fns, func(f Fn) int {
			return f.Weight
		})]
		if chosenFn.Equal(Empty()) {
			s.inTxn = true
			s.inflightStmts = 0
		}
	}
	return chosenFn.Eval(nil)
}

func (s *FnHookTxnWrap) AfterEvaluate(fn Fn, result string) string {
	if fn.Info != txnStartWrapName {
		return result
	}
	endTxnRs := s.endTxn()
	return result + " ; " + endTxnRs
}

func (s *FnHookTxnWrap) endTxn() string {
	fns := []Fn{
		Empty(),
		Or(
			Str("commit"),
			Str("rollback"),
		),
	}
	var fnIdx int
	if !s.inTxn {
		fnIdx = 0
	} else {
		inTxnW := s.ctl.MaxTxnStmtCount - s.inflightStmts
		if inTxnW < 0 {
			inTxnW = 0
		}
		outTxnW := s.inflightStmts
		fnIdx = randomSelectByFactor(fns, func(f Fn) int {
			if f.Equal(Empty()) {
				return inTxnW
			}
			return outTxnW
		})
	}
	chosenFn := fns[fnIdx]
	if chosenFn.Equal(Empty()) {
		s.inTxn = false
	} else if s.inTxn {
		s.inflightStmts++
	}
	return chosenFn.Eval(nil)
}

func NewFnHookTxnWrap(ctrl *ControlOption) *FnHookTxnWrap {
	return &FnHookTxnWrap{
		FnHookDefault: NewFnHookDefault("txn_wrap"),
		ctl:           ctrl,
	}
}
