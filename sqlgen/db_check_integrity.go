package sqlgen

var _ IntegrityChecker = (*State)(nil)
var _ IntegrityChecker = (*Table)(nil)
var _ IntegrityChecker = (*Column)(nil)
var _ IntegrityChecker = (*Index)(nil)

type IntegrityChecker interface {
	CheckIntegrity(state *State)
}

func (s *State) CheckIntegrity(state *State) {
	for _, tb := range s.tables {
		Assert(tb != nil)
		tb.CheckIntegrity(state)
	}
}

func (t *Table) CheckIntegrity(state *State) {
	for _, col := range t.Columns {
		Assert(col != nil)
		col.CheckIntegrity(state)
	}
	for _, idx := range t.Indices {
		Assert(idx != nil)
		idx.CheckIntegrity(state)
	}
}

func (c *Column) CheckIntegrity(state *State) {
	found := false
	for _, tb := range state.tables {
		if tb.ID == c.relatedTableID {
			found = true
			break
		}
	}
	Assert(found)
}

func (i *Index) CheckIntegrity(state *State) {
	for _, idxCol := range i.Columns {
		Assert(idxCol != nil)
	}
}
