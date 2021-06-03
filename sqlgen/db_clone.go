package sqlgen

import "log"

func (s *State) Clone() *State {
	if len(s.scope) != 1 {
		log.Printf("Clone failed with len(s.scope): %d != 1, it's in the middle state", len(s.scope))
		return nil
	}
	s1 := *s
	s1.tables = make([]*Table, 0, len(s.tables))
	for _, tbl := range s.tables {
		s1.tables = append(s1.tables, tbl.Clone())
	}
	s1.scope = make([]map[ScopeKeyType]ScopeObj, 0, len(s.scope))
	for _, v := range s.scope {
		temp := map[ScopeKeyType]ScopeObj{}
		for k, v := range v {
			temp[k] = v
		}
		s1.scope = append(s1.scope, temp)
	}
	return &s1
}

func (t *Table) Clone() *Table {
	newTable := *t
	newTable.Columns = make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		newTable.Columns = append(newTable.Columns, col.Clone())
	}
	newTable.values = cloneValues(t.values)
	newTable.Indices = make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		newIdx := *idx
		newIdx.Columns = make([]*Column, 0, len(idx.Columns))
		for _, c := range idx.Columns {
			offset := t.Columns.IndexByID(c.ID)
			newIdx.Columns = append(newIdx.Columns, newTable.Columns[offset])
		}
		newIdx.ColumnPrefix = cloneInts(idx.ColumnPrefix)
		newTable.Indices = append(newTable.Indices, &newIdx)
	}
	newTable.colForPrefixIndex = make([]*Column, 0, len(t.colForPrefixIndex))
	for _, c := range t.colForPrefixIndex {
		offset := t.Columns.IndexByID(c.ID)
		newTable.colForPrefixIndex = append(newTable.colForPrefixIndex, newTable.Columns[offset])
	}
	// TODO: DROP TABLE need to remove itself from children tables.
	newTable.childTables = []*Table{&newTable}
	return &newTable
}

func (c *Column) Clone() *Column {
	newCol := *c
	newCol.args = cloneStrings(c.args)
	return &newCol
}

func cloneValues(values [][]string) [][]string {
	ret := make([][]string, 0, len(values))
	for _, vs := range values {
		ret = append(ret, cloneStrings(vs))
	}
	return ret
}

func cloneStrings(ss []string) []string {
	ret := make([]string, 0, len(ss))
	for _, s := range ss {
		ret = append(ret, s)
	}
	return ret
}

func cloneInts(is []int) []int {
	ret := make([]int, 0, len(is))
	for _, i := range is {
		ret = append(ret, i)
	}
	return ret
}
