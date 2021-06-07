package sqlgen

import (
	"math/rand"
	"strings"
)

type State struct {
	hooks  []FnEvaluateHook
	weight map[string]int
	repeat map[string]Interval

	tables Tables
	ctes   [][]*Table
	scope  []map[ScopeKeyType]ScopeObj
	config map[ConfigKeyType]ScopeObj

	prepareStmts []*Prepare

	todoSQLs             []string
	invalid              bool
	fnStack              string
	lastBrokenAssumption string
}

type Table struct {
	ID      int
	Name    string
	AsName  string
	Columns Columns
	Indices []*Index
	Collate CollationType

	containsPK        bool // to ensure at most 1 pk in each table
	values            [][]string
	colForPrefixIndex Columns

	// childTables records tables that have the same structure.
	// A table is also its childTables.
	// This is used for SELECT OUT FILE and LOAD DATA.
	childTables []*Table
}

type Column struct {
	ID   int
	Name string
	Tp   ColumnType

	isUnsigned bool
	arg1       int      // optional
	arg2       int      // optional
	args       []string // for ColumnTypeSet and ColumnTypeEnum

	defaultVal string
	isNotNull  bool
	collate    CollationType
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

type ScopeObj struct {
	obj interface{}
}

func NewState() *State {
	s := &State{
		weight: make(map[string]int),
		repeat: make(map[string]Interval),
		config: make(map[ConfigKeyType]ScopeObj),
	}
	s.CreateScope() // create a root scope.
	s.AppendHook(NewFnHookScope(s))
	s.StoreConfig(ConfigKeyUnitAvoidAlterPKColumn, struct{}{})
	s.StoreConfig(ConfigKeyUnitLimitIndexKeyLength, struct{}{})
	s.StoreConfig(ConfigKeyUnitAvoidDropPrimaryKey, struct{}{})
	// s.AppendHook(NewFnHookTxnWrap(20))
	return s
}

func (s ScopeObj) IsNil() bool {
	return s.obj == nil
}

func (s ScopeObj) ToTable() *Table {
	return s.obj.(*Table)
}

func (s ScopeObj) ToTables() Tables {
	return s.obj.(Tables)
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

func (s ScopeObj) ToBoolOrDefault(d bool) bool {
	if s.obj == nil {
		return d
	}
	return s.obj.(bool)
}

func (s ScopeObj) ToString() string {
	return s.obj.(string)
}

func (s ScopeObj) ToIntOrDefault(defau1t int) int {
	if s.obj == nil {
		return defau1t
	}
	return s.ToInt()
}

func (s ScopeObj) ToStringOrDefault(defau1t string) string {
	if s.obj == nil {
		return defau1t
	}
	return s.ToString()
}

func (s ScopeObj) ToColumns() []*Column {
	return s.obj.([]*Column)
}

func (s ScopeObj) ToPrepare() *Prepare {
	return s.obj.(*Prepare)
}

func (s ScopeObj) ToTableColumnPairs() TableColumnPairs {
	return s.obj.(TableColumnPairs)
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

func (s *State) Store(key ScopeKeyType, val interface{}) {
	obj := ScopeObj{val}
	Assert(!obj.IsNil(), "storing a nil object")
	current := s.scope[len(s.scope)-1]
	current[key] = obj
}

func (s *State) StoreConfig(key ConfigKeyType, val interface{}) {
	obj := ScopeObj{val}
	Assert(!obj.IsNil(), "storing a nil object")
	s.config[key] = obj
}

func (s *State) StoreInRoot(key ScopeKeyType, val interface{}) {
	s.scope[0][key] = ScopeObj{val}
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

func (s *State) Roll(key ConfigKeyType, defaultVal int) bool {
	baseline := s.config[key].ToIntOrDefault(defaultVal)
	return rand.Intn(ProbabilityMax) < baseline
}

func (s *State) GetWeight(fn Fn) int {
	if w, ok := s.weight[fn.Info]; ok {
		return w
	}
	return fn.Weight
}

func (s *State) Clear(option ...StateClearOption) {
	for _, opt := range option {
		switch opt {
		case StateClearOptionWeight:
			s.weight = map[string]int{}
		case StateClearOptionRepeat:
			s.repeat = map[string]Interval{}
		case StateClearOptionConfig:
			s.config = map[ConfigKeyType]ScopeObj{}
		case StateClearOptionScope:
			s.scope = nil
			s.CreateScope()
		case StateClearOptionAll:
			s.weight = map[string]int{}
			s.repeat = map[string]Interval{}
			s.config = map[ConfigKeyType]ScopeObj{}
			s.scope = nil
			s.CreateScope()
		}
	}
}

func (s *State) GetCurrentStack() string {
	var sb strings.Builder
	for i := 0; i < len(s.scope); i++ {
		if i > 0 {
			sb.WriteString("-")
		}
		sb.WriteString("'")
		if currentFn, ok := s.scope[i][ScopeKeyCurrentFn]; ok {
			sb.WriteString(currentFn.ToString())
		} else {
			sb.WriteString("root")
		}
		sb.WriteString("'")
	}
	return sb.String()
}

func (s *State) LastBrokenAssumption() string {
	return s.lastBrokenAssumption
}

func (s *State) GetRepeat(fn Fn) (lower int, upper int) {
	if w, ok := s.repeat[fn.Info]; ok {
		return w.lower, w.upper
	}
	return fn.Repeat.lower, fn.Repeat.upper
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

func (s *State) ExistsConfig(key ConfigKeyType) bool {
	_, ok := s.config[key]
	return ok
}

func (s *State) SearchConfig(key ConfigKeyType) ScopeObj {
	return s.config[key]
}

func (s *State) Exists(key ScopeKeyType) bool {
	return !s.Search(key).IsNil()
}

func (s *State) AllocGlobalID(key ScopeKeyType) int {
	var result int

	if v, ok := s.scope[0][key]; ok {
		result = v.ToInt()
	} else {
		result = 0
	}
	s.scope[0][key] = ScopeObj{result + 1}
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

func (s *State) IsValid() bool {
	return !s.invalid
}

func (s *State) SetValid(valid bool) {
	s.invalid = !valid
}
