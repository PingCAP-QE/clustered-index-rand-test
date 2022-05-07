package sqlgen

var NoTooMuchTables = func(s *State) bool {
	return len(s.tables) < 20
}

var CurrentTableHasIndices = func(s *State) bool {
	return len(s.env.Table.Indices) > 0
}

var NoPrimaryKey = func(s *State) bool {
	idx := s.env.Table.Indices
	return !idx.Find(func(index *Index) bool {
		return index.Tp == IndexTypePrimary
	})
}

var HasTables = func(s *State) bool {
	return len(s.tables) >= 1
}

var MoreThan1Columns = func(s *State) bool {
	tbl := s.env.Table
	return len(tbl.Columns) > 1
}

var HasDroppableColumn = func(s *State) bool {
	tbl := s.env.Table
	for _, c := range tbl.Columns {
		if !c.ColumnHasIndex(tbl) {
			return true
		}
	}
	return false
}

var IndexColumnPrefixable = func(s *State) bool {
	col := s.env.IdxColumn
	return col.Tp.IsStringType() && col.arg1 > 0
}

var IndexColumnCanHaveNoPrefix = func(s *State) bool {
	col := s.env.IdxColumn
	return !col.Tp.NeedKeyLength()
}

var HasNonPKCol = func(s *State) bool {
	tbl := s.env.Table
	pk := tbl.GetPrimaryKeyIndex()
	if pk == nil {
		return true
	}
	return tbl.Columns.Find(func(c *Column) bool {
		return !pk.ContainsColumn(c)
	})
}

var HasSameColumnType = func(s *State) bool {
	col := s.env.Column
	t, _ := s.GetRandTableColumnWithTp(col.Tp)
	return t != nil
}
