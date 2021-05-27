package sqlgen

var _ IntegrityChecker = (*State)(nil)
var _ IntegrityChecker = (*Table)(nil)
var _ IntegrityChecker = (*Column)(nil)
var _ IntegrityChecker = (*Index)(nil)

type IntegrityChecker interface {
	CheckIntegrity()
}

func (s *State) CheckIntegrity() {
	for _, tb := range s.tables {
		Assert(tb != nil)
		tb.CheckIntegrity()
	}
}

func (t *Table) CheckIntegrity() {
	for _, col := range t.Columns {
		Assert(col != nil)
		col.CheckIntegrity()
	}
	for _, idx := range t.Indices {
		Assert(idx != nil)
		idx.CheckIntegrity()
		for _, idxCol := range idx.Columns {
			Assert(t.ContainsColumn(idxCol))
		}
	}
}

func (c *Column) CheckIntegrity() {
}

func (i *Index) CheckIntegrity() {
	for _, idxCol := range i.Columns {
		Assert(idxCol != nil)
	}
}
