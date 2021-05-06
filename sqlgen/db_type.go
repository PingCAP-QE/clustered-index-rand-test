package sqlgen

import "math/rand"

type State struct {
	ctrl *ControlOption

	tables           []*Table
	scope            []map[ScopeKeyType]ScopeObj
	enabledClustered bool

	prepareStmts []*Prepare

	finishInit bool
	todoSQLs   []string

	ctes [][]*CTE
}

type Table struct {
	Id      int
	Name    string
	Columns []*Column
	Indices []*Index

	containsPK        bool // to ensure at most 1 pk in each table
	PartitionColumns  []*Column
	values            [][]string
	colForPrefixIndex []*Column

	// childTables records tables that have the same structure.
	// A table is also its childTables.
	// This is used for SELECT OUT FILE and LOAD DATA.
	childTables []*Table
}

type Column struct {
	Id   int
	Name string
	Tp   ColumnType

	isUnsigned bool
	arg1       int      // optional
	arg2       int      // optional
	args       []string // for ColumnTypeSet and ColumnTypeEnum

	defaultVal     string
	isNotNull      bool
	relatedIndices map[int]struct{}
	collate        CollationType
}

type CTE struct {
	Name   string
	AsName string
	Cols   []*Column
}

type Index struct {
	Id           int
	Name         string
	Tp           IndexType
	Columns      []*Column
	ColumnPrefix []int
}

type Prepare struct {
	Id   int
	Name string
	Args []func() string
}

func NewState(opts ...func(ctl *ControlOption)) *State {
	s := &State{ctrl: DefaultControlOption()}
	for _, opt := range opts {
		opt(s.ctrl)
	}
	s.enabledClustered = s.ctrl.EnableTestTiFlash
	s.CreateScope()
	return s
}

func NewState2(EnableTestTiFlash bool) *State {
	return NewState(func(ctl *ControlOption) {
		ctl.EnableTestTiFlash = EnableTestTiFlash
	})
}

type ScopeObj struct {
	obj interface{}
}

func NewScopeObj(obj interface{}) ScopeObj {
	return ScopeObj{obj}
}

func (s ScopeObj) IsNil() bool {
	return s.obj == nil
}

func (s ScopeObj) ToTable() *Table {
	return s.obj.(*Table)
}

func (s ScopeObj) ToTables() []*Table {
	return s.obj.([]*Table)
}

func (s ScopeObj) ToColumn() *Column {
	return s.obj.(*Column)
}

func (s ScopeObj) ToIndex() *Index {
	return s.obj.(*Index)
}

func (s ScopeObj) ToInt() int {
	return s.obj.(int)
}

func (s ScopeObj) ToBool() bool {
	return s.obj.(bool)
}

func (s ScopeObj) ToColumns() []*Column {
	return s.obj.([]*Column)
}

func (s ScopeObj) ToPrepare() *Prepare {
	return s.obj.(*Prepare)
}

func (s *State) CreateScope() {
	s.scope = append(s.scope, make(map[ScopeKeyType]ScopeObj))
}

func (s *State) DestroyScope() {
	if len(s.scope) == 0 {
		return
	}
	s.scope = s.scope[:len(s.scope)-1]
}

func (s *State) Store(key ScopeKeyType, val ScopeObj) {
	Assert(!val.IsNil(), "storing a nil object")
	current := s.scope[len(s.scope)-1]
	current[key] = val
}

func (s *State) StoreInParent(key ScopeKeyType, val ScopeObj) {
	Assert(len(s.scope) > 1, "cannot StoreInParent in the root scope")
	Assert(!val.IsNil(), "storing a nil object")
	current := s.scope[len(s.scope)-2]
	current[key] = val
}

func (s *State) StoreInRoot(key ScopeKeyType, val ScopeObj) {
	s.scope[0][key] = val
}

func (s *State) Search(key ScopeKeyType) ScopeObj {
	for i := len(s.scope) - 1; i >= 0; i-- {
		current := s.scope[i]
		if v, ok := current[key]; ok {
			return v
		}
	}
	return ScopeObj{}
}

func (s *State) CreateScopeAndStore(key ScopeKeyType, val ScopeObj) {
	s.CreateScope()
	s.Store(key, val)
}

func (s *State) AllocGlobalID(key ScopeKeyType) int {
	var result int

	if v, ok := s.scope[0][key]; ok {
		result = v.ToInt()
	} else {
		result = 0
	}
	s.scope[0][key] = NewScopeObj(result + 1)
	return result
}

func (s *State) PickRandomCTEOrTableName() string {
	names := make([]string, 0, 10)
	for _, cteL := range s.ctes {
		for _, cte := range cteL {
			names = append(names, cte.Name)
		}
	}

	for _, tbl := range s.tables {
		names = append(names, tbl.Name)
	}

	return names[rand.Intn(len(names))]
}

func (s *State) GetRandomCTE() *CTE {
	ctes := make([]*CTE, 0, 10)
	for _, cteL := range s.ctes {
		for _, cte := range cteL {
			ctes = append(ctes, cte)
		}
	}

	return ctes[rand.Intn(len(ctes))]
}

func (s *State) GetCTECount() int {
	c := 0
	for _, cteL := range s.ctes {
		c += len(cteL)
	}

	return c
}

type ScopeListener struct {
	state *State
}

func (s *ScopeListener) BeforeProductionGen(fn *Fn) {
	s.state.CreateScope()
}

func (s *ScopeListener) AfterProductionGen(fn *Fn, result *Result) {
	s.state.DestroyScope()
}

func (s *ScopeListener) ProductionCancel(fn *Fn) {
	return
}

type PostListener struct {
	callbacks map[string]func()
}

func (p *PostListener) BeforeProductionGen(fn *Fn) {}

func (p *PostListener) AfterProductionGen(fn *Fn, result *Result) {
	if f, ok := p.callbacks[fn.Name]; ok {
		f()
		delete(p.callbacks, fn.Name)
	}
}

func (p *PostListener) ProductionCancel(fn *Fn) {}

func (p *PostListener) Register(fnName string, fn func()) {
	p.callbacks[fnName] = fn
}

type DebugListener struct {
	parentsFn      []string
	obtainedResult []*Result
}

func (d *DebugListener) BeforeProductionGen(fn *Fn) {
	d.parentsFn = append(d.parentsFn, fn.Name)
}

func (d *DebugListener) AfterProductionGen(fn *Fn, result *Result) {
	d.parentsFn = d.parentsFn[:len(d.parentsFn)-1]
	d.obtainedResult = append(d.obtainedResult, result)
}

func (d *DebugListener) ProductionCancel(fn *Fn) {
}

type TxnListener struct {
	inTxn         bool
	inflightStmts int
	ctl           *ControlOption
}

const txnStartWrapName = "txnWrappedStart"

func (s *TxnListener) BeforeProductionGen(fn *Fn) {
	if fn.Name != "start" {
		return
	}
	actualFn := *fn
	*fn = Fn{
		Name: txnStartWrapName,
		F: func() Result {
			startTxnRs := s.startTxn()
			if startTxnRs.Tp != PlainString {
				return InvalidResult()
			}
			currRs := actualFn.F()
			if currRs.Tp != PlainString {
				return InvalidResult()
			}
			if len(startTxnRs.Value) == 0 {
				return currRs
			}
			return Result{
				Tp:    PlainString,
				Value: startTxnRs.Value + " ; " + currRs.Value,
			}
		},
	}
	return
}

func (s *TxnListener) startTxn() Result {
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
		if chosenFn.Name != Empty().Name {
			s.inTxn = true
			s.inflightStmts = 0
		}
	}
	return evaluateFn(chosenFn)
}

func (s *TxnListener) AfterProductionGen(fn *Fn, result *Result) {
	if fn.Name != txnStartWrapName {
		return
	}
	if result.Tp != PlainString {
		return
	}
	endTxnRs := s.endTxn()
	if endTxnRs.Tp != PlainString || len(endTxnRs.Value) == 0 {
		return
	}
	*result = Result{
		Tp:    result.Tp,
		Value: result.Value + " ; " + endTxnRs.Value,
	}
	return
}

func (s *TxnListener) endTxn() Result {
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
			if f.Name == Empty().Name {
				return inTxnW
			}
			return outTxnW
		})
	}
	chosenFn := fns[fnIdx]
	if chosenFn.Name != Empty().Name {
		s.inTxn = false
	} else if s.inTxn {
		s.inflightStmts++
	}
	rs := evaluateFn(chosenFn)
	if rs.Tp == PlainString {
		return rs
	}
	return InvalidResult()
}

func (s TxnListener) ProductionCancel(fn *Fn) {
	return
}
