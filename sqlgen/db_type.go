package sqlgen

type State struct {
	ctrl        *ControlOption
	fnListeners []ProductionListener

	tables           []*Table
	scope            []map[ScopeKeyType]ScopeObj
	enabledClustered bool

	prepareStmts []*Prepare

	finishInit bool
	todoSQLs   []string
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
	s := &State{
		ctrl: DefaultControlOption(),
	}
	for _, opt := range opts {
		opt(s.ctrl)
	}
	s.enabledClustered = s.ctrl.EnableTestTiFlash
	s.fnListeners = []ProductionListener{&ScopeListener{state: s}}
	s.CreateScope()
	if s.ctrl.AttachToTxn {
		s.fnListeners = append(s.fnListeners, &TxnListener{ctl: s.ctrl})
	}
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

var _ ProductionListener = (*ScopeListener)(nil)

type ScopeListener struct {
	state *State
}

func (s *ScopeListener) BeforeProductionGen(fn Fn) Fn {
	s.state.CreateScope()
	return fn
}

func (s *ScopeListener) AfterProductionGen(fn Fn, result string) string {
	s.state.DestroyScope()
	return result
}

var _ ProductionListener = (*PostListener)(nil)

type PostListener struct {
	callbacks map[string]func()
}

func (p *PostListener) BeforeProductionGen(fn Fn) Fn {
	return fn
}

func (p *PostListener) AfterProductionGen(fn Fn, result string) string {
	return result
}

func (p *PostListener) Register(fnName string, fn func()) {
	p.callbacks[fnName] = fn
}

var _ ProductionListener = (*DebugListener)(nil)

type DebugListener struct {
	parentsFn      []string
	obtainedResult []string
}

func (d *DebugListener) BeforeProductionGen(fn Fn) Fn {
	d.parentsFn = append(d.parentsFn, fn.Info)
	return fn
}

func (d *DebugListener) AfterProductionGen(fn Fn, result string) string {
	d.parentsFn = d.parentsFn[:len(d.parentsFn)-1]
	d.obtainedResult = append(d.obtainedResult, result)
	return result
}

type TxnListener struct {
	inTxn         bool
	inflightStmts int
	ctl           *ControlOption
}

const txnStartWrapName = "txnWrappedStart"

func (s *TxnListener) BeforeProductionGen(fn Fn) Fn {
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

func (s *TxnListener) startTxn() string {
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

func (s *TxnListener) AfterProductionGen(fn Fn, result string) string {
	if fn.Info != txnStartWrapName {
		return result
	}
	endTxnRs := s.endTxn()
	return result + " ; " + endTxnRs
}

func (s *TxnListener) endTxn() string {
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
