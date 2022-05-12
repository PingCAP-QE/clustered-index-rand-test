package sqlgen

var _ FnEvaluateHook = (*FnHookTxnWrap)(nil)

type FnHookTxnWrap struct {
	FnHookDefault
	maxTxnStmtCount int
	inTxn           bool
	inflightStmts   int
	state           *State
}

const txnStartWrapName = "txnWrappedStart"

func (s *FnHookTxnWrap) BeforeEvaluate(state *State, fn Fn) Fn {
	if fn.Info == Start.Info {
		return fn
	}
	return Fn{
		Info: txnStartWrapName,
		Gen: func(state *State) (string, error) {
			startTxnRs, err := s.startTxn()
			if err != nil {
				return "", err
			}
			currRs, err := fn.Gen(state)
			if err != nil {
				return "", err
			}
			return startTxnRs + " ; " + currRs, nil
		},
	}
}

func (s *FnHookTxnWrap) startTxn() (string, error) {
	fns := []Fn{
		Empty.W(1),
		And(
			Str("begin"),
			Or(
				Str("pessimistic"),
				Str("optimistic"),
			),
		).W(1),
	}
	var chosenFn Fn
	if s.inTxn {
		// inside txn, never begin again.
		chosenFn = fns[0]
	} else {
		chosenFn = fns[randomSelectByFactor(fns, func(f Fn) int {
			return f.Weight
		})]
		if chosenFn.Equal(Empty) {
			s.inTxn = true
			s.inflightStmts = 0
		}
	}
	return chosenFn.Eval(s.state)
}

func (s *FnHookTxnWrap) AfterEvaluate(state *State, fn Fn, result string) string {
	if fn.Info != txnStartWrapName {
		return result
	}
	endTxnRs, err := s.endTxn()
	if err != nil {
		return ""
	}
	return result + " ; " + endTxnRs
}

func (s *FnHookTxnWrap) endTxn() (string, error) {
	fns := []Fn{
		Empty,
		Or(
			Str("commit"),
			Str("rollback"),
		),
	}
	var fnIdx int
	if !s.inTxn {
		fnIdx = 0
	} else {
		inTxnW := s.maxTxnStmtCount - s.inflightStmts
		if inTxnW < 0 {
			inTxnW = 0
		}
		outTxnW := s.inflightStmts
		fnIdx = randomSelectByFactor(fns, func(f Fn) int {
			if f.Equal(Empty) {
				return inTxnW
			}
			return outTxnW
		})
	}
	chosenFn := fns[fnIdx]
	if chosenFn.Equal(Empty) {
		s.inTxn = false
	} else if s.inTxn {
		s.inflightStmts++
	}
	return chosenFn.Eval(s.state)
}

func NewFnHookTxnWrap(state *State, maxTxnStmtCount int) *FnHookTxnWrap {
	return &FnHookTxnWrap{
		FnHookDefault:   NewFnHookDefault("txn_wrap"),
		maxTxnStmtCount: maxTxnStmtCount,
		state:           state,
	}
}
