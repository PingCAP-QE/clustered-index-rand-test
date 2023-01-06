package sqlgen

import (
	"math/rand"
)

type State struct {
	hooks  *Hooks
	weight map[string]int
	repeat map[string]Interval
	prereq map[string]func(*State) bool

	Tables        Tables
	droppedTables Tables

	ctes  [][]*Table
	alloc *IDAllocator

	env *Env

	prepareStmts []*Prepare

	fnStack string
}

type Table struct {
	ID        int
	Name      string
	AsName    string
	Columns   Columns
	Indexes   Indexes
	Collate   *Collation
	Clustered bool

	TiflashReplica int

	Values            [][]string
	ColForPrefixIndex Columns

	// ChildTables records tables that have the same structure.
	// A table is also its ChildTables.
	// This is used for SELECT OUT FILE and LOAD DATA.
	ChildTables []*Table
}

type Column struct {
	ID        int
	Name      string
	Tp        ColumnType
	Collation *Collation

	IsUnsigned bool
	Arg1       int // optional
	Arg2       int // optional

	Args       []string // for ColumnTypeSet and ColumnTypeEnum
	DefaultVal string
	IsNotNull  bool
}

type Index struct {
	ID           int
	Name         string
	Tp           IndexType
	Columns      Columns
	ColumnPrefix []int
}

type Prepare struct {
	ID   int
	Name string
	Args []func() string
}

func NewState() *State {
	s := &State{
		hooks:  &Hooks{},
		weight: make(map[string]int),
		repeat: make(map[string]Interval),
		prereq: make(map[string]func(*State) bool),
		alloc:  &IDAllocator{},
		env:    &Env{},
	}
	s.hooks.Append(NewFnHookScope(s))
	return s
}

func (s *State) Hook() *Hooks {
	return s.hooks
}

func (s *State) Env() *Env {
	return s.env
}

func (s *State) Config() *ConfigurableState {
	return (*ConfigurableState)(s)
}

func (s *State) ReplaceRule(fn Fn, newFn Fn) {
	replacer := s.hooks.Find(HookNameReplacer)
	if replacer == nil {
		replacer = NewFnHookReplacer()
		s.hooks.Append(replacer)
	}
	replacer.(*FnHookReplacer).Replace(fn, newFn)
}

func (s *State) CleanReplaceRule(fn Fn) {
	replacer := s.hooks.Find(HookNameReplacer)
	if replacer == nil {
		return
	}
	replacer.(*FnHookReplacer).RemoveReplace(fn)
}

func (s *State) GetWeight(fn Fn) int {
	if !s.GetPrerequisite(fn)(s) {
		return 0
	}
	if w, ok := s.weight[fn.Info]; ok {
		return w
	}
	return fn.Weight
}

func (s *State) GetRepeat(fn Fn) (lower int, upper int) {
	if w, ok := s.repeat[fn.Info]; ok {
		return w.lower, w.upper
	}
	return fn.Repeat.lower, fn.Repeat.upper
}

func (s *State) GetPrerequisite(fn Fn) func(state *State) bool {
	if p, ok := s.prereq[fn.Info]; ok {
		return p
	}
	if fn.Prerequisite != nil {
		return fn.Prerequisite
	}
	return func(state *State) bool {
		return true
	}
}

func (s *State) RemoveRepeat(fn Fn) {
	if _, ok := s.repeat[fn.Info]; ok {
		delete(s.repeat, fn.Info)
	}
}

func (s *State) RemoveWeight(fn Fn) {
	if _, ok := s.weight[fn.Info]; ok {
		delete(s.weight, fn.Info)
	}
}

func (s *State) PickRandomCTEOrTableName() string {
	names := make([]string, 0, 10)
	for _, cteL := range s.ctes {
		for _, cte := range cteL {
			names = append(names, cte.Name)
		}
	}

	for _, tbl := range s.Tables {
		names = append(names, tbl.Name)
	}

	return names[rand.Intn(len(names))]
}

func (s *State) GetRandomCTE() *Table {
	ctes := make([]*Table, 0, 10)
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

// QueryState represent an intermediate state during a query generation.
type QueryState struct {
	SelectedCols map[*Table]QueryStateColumns
	IsWindow     bool
	FieldNumHint int
}

type QueryStateColumns struct {
	Columns
	Attr []string
}

func (q QueryState) GetRandTable() *Table {
	idx := rand.Intn(len(q.SelectedCols))
	for t := range q.SelectedCols {
		if idx == 0 {
			return t
		}
		idx--
	}
	return nil
}

type MultiObjs struct {
	items []string
}

func NewMultiObjs() *MultiObjs {
	return &MultiObjs{}
}

func (m *MultiObjs) SameObject(name string) bool {
	if m == nil {
		return false
	}
	for _, i := range m.items {
		if i == name {
			return true
		}
	}
	return false
}

func (m *MultiObjs) AddName(name string) {
	m.items = append(m.items, name)
}
