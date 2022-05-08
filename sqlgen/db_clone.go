package sqlgen

import (
	"log"
)

func (s *State) Clone() *State {
	if s.env.Depth() > 1 {
		log.Printf("Clone failed with len(s.scope): %d != 1, it's in the middle state", s.env.Depth())
		return nil
	}
	s1 := *s
	s1.Tables = make([]*Table, 0, len(s.Tables))
	for _, tbl := range s.Tables {
		s1.Tables = append(s1.Tables, tbl.Clone())
	}
	s1.env = s.env.Clone()
	return &s1
}

func (t *Table) Clone() *Table {
	newTable := *t
	newTable.Columns = make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		newTable.Columns = append(newTable.Columns, col.Clone())
	}
	newTable.values = cloneValues(t.values)
	newTable.Indexes = make([]*Index, 0, len(t.Indexes))
	for _, idx := range t.Indexes {
		newIdx := *idx
		newIdx.Columns = make([]*Column, 0, len(idx.Columns))
		for _, c := range idx.Columns {
			offset := t.Columns.ByID(c.ID)
			newIdx.Columns = append(newIdx.Columns, newTable.Columns[offset])
		}
		newIdx.ColumnPrefix = cloneInts(idx.ColumnPrefix)
		newTable.Indexes = append(newTable.Indexes, &newIdx)
	}
	newTable.colForPrefixIndex = make([]*Column, 0, len(t.colForPrefixIndex))
	for _, c := range t.colForPrefixIndex {
		offset := t.Columns.ByID(c.ID)
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
