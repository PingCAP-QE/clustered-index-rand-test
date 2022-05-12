package sqlgen

var NoTooMuchTables = func(s *State) bool {
	return len(s.Tables) < 20
}

var CurrentTableHasIndices = func(s *State) bool {
	return len(s.env.Table.Indexes) > 0
}

var NoPrimaryKey = func(s *State) bool {
	idx := s.env.Table.Indexes
	return !idx.Find(func(index *Index) bool {
		return index.Tp == IndexTypePrimary
	})
}

var HasTables = func(s *State) bool {
	return len(s.Tables) >= 1
}

var HasDroppedTables = func(s *State) bool {
	return len(s.droppedTables) > 0
}

var MoreThan1Columns = func(s *State) bool {
	tbl := s.env.Table
	return len(tbl.Columns) > 1
}

var HasDroppableColumn = func(s *State) bool {
	tbl := s.env.Table
	for _, c := range tbl.Columns {
		if !c.HasIndex(tbl) {
			return true
		}
	}
	return false
}

var HasShardableColumn = func(s *State) bool {
	tbl := s.env.Table
	if tbl == nil {
		return false
	}
	if tbl.Columns == nil {
		return false
	}
	return tbl.Indexes.Find(func(i *Index) bool {
		return i.Columns.Found(isShardableColumn)
	})
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
	pk := tbl.Indexes.Primary()
	if pk == nil {
		return true
	}
	return tbl.Columns.Found(func(c *Column) bool {
		return !pk.HasColumn(c)
	})
}

var isShardableColumn = func(c *Column) bool {
	return c.Tp != ColumnTypeJSON && c.Tp != ColumnTypeSet && c.Tp != ColumnTypeBit && c.Tp != ColumnTypeEnum
}
var HasSameColumnType = func(s *State) bool {
	col := s.env.Column
	t, _ := GetRandTableColumnWithTp(s.Tables, col.Tp)
	return t != nil
}
