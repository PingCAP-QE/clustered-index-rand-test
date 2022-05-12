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

var HasSameColumnType = func(s *State) bool {
	col := s.env.Column
	t, _ := GetRandTableColumnWithTp(s.Tables, col.Tp)
	return t != nil
}

var ModifyColumnCompatible = func(oldCol *Column, newColTp ColumnType) bool {
	if oldCol == nil {
		return true
	}
	oldTp := oldCol.Tp
	if oldTp.IsTimeType() || oldTp.IsStringType() || oldTp == ColumnTypeJSON {
		if newColTp == ColumnTypeBit {
			return false
		}
	}
	if oldTp.IsTimeType() || oldTp.IsStringType() || oldTp.IsFloatingType() || oldTp == ColumnTypeJSON || oldTp == ColumnTypeBit {
		if newColTp == ColumnTypeEnum || newColTp == ColumnTypeSet {
			return false
		}
	}
	if oldTp == ColumnTypeEnum || oldTp == ColumnTypeSet || oldTp.IsFloatingType() || oldTp == ColumnTypeBit {
		if newColTp.IsTimeType() {
			return false
		}
	}
	return true
}

var CharsetCompatible = func(from, to *Column) bool {
	if from == nil {
		return true
	}
	if from.Collation == nil || to.Collation == nil {
		return true
	}
	incompatibleSets := map[string][]string{
		"binary":  {"gbk"},
		"utf8mb4": {"binary"},
		"gbk":     {"binary"},
	}
	if ss, ok := incompatibleSets[from.Collation.CharsetName]; ok {
		for _, s := range ss {
			if s == to.Collation.CharsetName {
				return false
			}
		}
	}
	return true
}
