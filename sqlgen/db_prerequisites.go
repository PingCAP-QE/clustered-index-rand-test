package sqlgen

var NoTooMuchTables = func(s *State) bool {
	return len(s.Tables) < 20
}

var HasModifiableIndexes = func(s *State) bool {
	cnt := len(s.Env().Table.Indexes)
	switch cnt {
	case 0:
		return false
	case 1:
		return !s.Env().Table.Clustered
	default:
		return true
	}
}

var AddIndex2 Fn

func init() {
	AddIndex2 = AddIndex
}

var NotAddingIndex = func(state *State) bool {
	return !state.Env().IsIn(AddIndex2)
}

var NoPrimaryKey = func(s *State) bool {
	idx := s.env.Table.Indexes
	return !idx.Found(func(index *Index) bool {
		return index.Tp == IndexTypePrimary
	})
}

var HasNotNullColumn = func(s *State) bool {
	tbl := s.env.Table
	return tbl.Columns.Found(func(c *Column) bool {
		return c.isNotNull
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

var IndexColumnPrefixable = func(s *State) bool {
	col := s.env.IdxColumn
	return col.Tp.IsStringType() && col.arg1 > 0
}

var IndexColumnCanHaveNoPrefix = func(s *State) bool {
	col := s.env.IdxColumn
	return !col.Tp.NeedKeyLength()
}

var HasModifiableColumn = func(s *State) bool {
	tbl := s.env.Table
	cols := tbl.Columns.Filter(func(c *Column) bool {
		// Not support operate the same object in multi-schema change.
		return !s.env.MultiObjs.SameObject(c.Name)
	})
	pk := tbl.Indexes.Primary()
	if pk != nil && tbl.Clustered {
		// Not support modify/change clustered primary key columns.
		cols = cols.Diff(pk.Columns)
	}
	return len(cols) > 0
}

var HasSameColumnType = func(s *State) bool {
	col := s.env.Column
	t, _ := GetRandTableColumnWithTp(s.Tables, col.Tp)
	return t != nil
}
