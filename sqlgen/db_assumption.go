package sqlgen

func (s *State) CheckAssumptions(fs ...func(*State) bool) bool {
	for _, f := range fs {
		if !f(s) {
			s.Invalidate()
			return false
		}
	}
	return true
}

var NoTooMuchTables = func(s *State) bool {
	return len(s.tables) < s.ctrl.MaxTableNum
}

var HasTables = func(s *State) bool {
	return len(s.tables) >= 1
}

var HasPreparedStmts = func(s *State) bool {
	return len(s.prepareStmts) >= 1
}

var CanReadGCSavePoint = func(s *State) bool {
	return s.ctrl.CanReadGCSavePoint
}

var EnabledSelectIntoAndLoad = func(s *State) bool {
	return s.ctrl.EnableSelectOutFileAndLoadData
}

var AlreadySelectOutfile = func(s *State) bool {
	return s.Exists(ScopeKeyLastOutFileTable)
}

var MoreThan1Columns = func(s *State) bool {
	tbl := s.Search(ScopeKeyCurrentTable).ToTable()
	return len(tbl.Columns) > 1
}

var HasKey = func(key ScopeKeyType) func(s *State) bool {
	return func(s *State) bool {
		if !s.Exists(key) {
			NeverReach()
		}
		return true
	}
}

var HasDroppableColumn = func(s *State) bool {
	tbl := s.Search(ScopeKeyCurrentTable).ToTable()
	for _, c := range tbl.Columns {
		if c.IsDroppable() {
			return true
		}
	}
	return false
}

var HasIndices = func(s *State) bool {
	tbl := s.Search(ScopeKeyCurrentTable).ToTable()
	return len(tbl.Indices) > 0
}

var EnableColumnTypeChange = func(s *State) bool {
	return s.ctrl.EnableColumnTypeChange
}
