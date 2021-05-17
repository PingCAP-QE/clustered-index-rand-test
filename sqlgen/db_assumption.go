package sqlgen

import (
	"reflect"
	"runtime"
)

func (s *State) CheckAssumptions(fs ...func(*State) bool) bool {
	for _, f := range fs {
		if !f(s) {
			s.lastBrokenAssumption = runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
			return false
		}
	}
	return true
}

func NoTooMuchTables(s *State) bool {
	maxTableCnt := s.SearchConfig(ConfigKeyIntMaxTableCount).ToIntOrDefault(20)
	return len(s.tables) < maxTableCnt
}

func HasTables(s *State) bool {
	return len(s.tables) >= 1
}

func HasAtLeast2Tables(s *State) bool {
	return len(s.tables) >= 2
}

func HasPreparedStmts(s *State) bool {
	return len(s.prepareStmts) >= 1
}

func CanReadGCSavePoint(s *State) bool {
	return s.ctrl.CanReadGCSavePoint
}

func EnabledSelectIntoAndLoad(s *State) bool {
	return s.ctrl.EnableSelectOutFileAndLoadData
}

func AlreadySelectOutfile(s *State) bool {
	return s.Exists(ScopeKeyLastOutFileTable)
}

func MoreThan1Columns(s *State) bool {
	tbl := s.Search(ScopeKeyCurrentTables).ToTables().One()
	return len(tbl.Columns) > 1
}

func MustHaveKey(key ScopeKeyType) func(s *State) bool {
	return func(s *State) bool {
		Assert(s.Exists(key))
		return true
	}
}

func HasDroppableColumn(s *State) bool {
	tbl := s.Search(ScopeKeyCurrentTables).ToTables().One()
	for _, c := range tbl.Columns {
		if c.IsDroppable() {
			return true
		}
	}
	return false
}

func HasIndices(s *State) bool {
	tbl := s.Search(ScopeKeyCurrentTables).ToTables().One()
	return len(tbl.Indices) > 0
}

func EnableColumnTypeChange(s *State) bool {
	return s.ctrl.EnableColumnTypeChange
}
