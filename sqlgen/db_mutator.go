package sqlgen

func (s *State) SetWeight(prod Fn, weight int) {
	Assert(weight >= 0)
	s.weight[prod.Info] = weight
}

func (s *State) SetRepeat(prod Fn, lower int, upper int) {
	Assert(lower > 0)
	Assert(lower <= upper)
	s.repeat[prod.Info] = Interval{lower, upper}
}

func (s *State) SetPrerequisite(fn Fn, pred func(*State) bool) {
	s.prereq[fn.Info] = pred
}

func (s *State) AppendTable(t *Table) {
	s.Tables = append(s.Tables, t)
}

func (s *State) RemoveTable(t *Table) {
	tmp := s.Tables[:0]
	for _, tb := range s.Tables {
		if tb.ID != t.ID {
			tmp = append(tmp, tb)
		} else {
			s.droppedTables = append(s.droppedTables, tb)
		}
	}
	s.Tables = tmp
}

func (s *State) TruncateTable(t *Table) {
	newTable := t.CloneCreateTableLike(s)
	newTable.Name = t.Name
	for i, st := range s.Tables {
		if st == t {
			s.Tables[i] = newTable
			break
		}
	}
	s.droppedTables = append(s.droppedTables, t)
}

func (s *State) FlashbackTable(t *Table) {
	s.droppedTables = s.droppedTables.Removed(t)
	for i, st := range s.Tables {
		// For truncated tables.
		if st.Name == t.Name {
			s.Tables[i] = t
			return
		}
	}
	// For dropped tables.
	s.AppendTable(t)
}

func (s *State) PushCTE(cte *Table) {
	s.ctes[len(s.ctes)-1] = append(s.ctes[len(s.ctes)-1], cte)
}

func (s *State) PopCTE() []*Table {
	if len(s.ctes) == 0 {
		panic("0 CTEs")
	}
	tc := s.ctes[len(s.ctes)-1]
	s.ctes = s.ctes[:len(s.ctes)-1]
	return tc
}

func (s *State) CurrentCTE() *Table {
	if len(s.ctes) == 0 {
		return nil
	}
	l := s.ctes[len(s.ctes)-1]
	if len(l) == 0 {
		return nil
	}
	return l[len(l)-1]
}

func (s *State) LastCTEs() []*Table {
	return s.ctes[len(s.ctes)-1]
}

func (s *State) ParentCTEColCount() int {
	if len(s.ctes) < 2 {
		return 0
	}
	ctes := s.ctes[len(s.ctes)-2]
	return len(ctes[len(ctes)-1].Columns)
}

func (s *State) ParentCTE() *Table {
	if len(s.ctes) < 2 {
		return nil
	}
	ctes := s.ctes[len(s.ctes)-2]
	return ctes[len(ctes)-1]
}

func (s *State) AppendPrepare(pre *Prepare) {
	s.prepareStmts = append(s.prepareStmts, pre)
}

func (s *State) RemovePrepare(p *Prepare) {
	var pos int
	for i := range s.prepareStmts {
		if s.prepareStmts[i].ID == p.ID {
			pos = i
			break
		}
	}
	s.prepareStmts = append(s.prepareStmts[:pos], s.prepareStmts[pos+1:]...)
}

func (t *Table) AppendColumn(c *Column) {
	t.Columns = append(t.Columns, c)
	for i := range t.Values {
		t.Values[i] = append(t.Values[i], c.ZeroValue())
	}
}

func (t *Table) ModifyColumn(oldCol, newCol *Column) {
	for i, c := range t.Columns {
		if c.ID == oldCol.ID {
			t.Columns[i] = newCol
			break
		}
	}
	for _, idx := range t.Indexes {
		for j, idxCol := range idx.Columns {
			if idxCol.ID == oldCol.ID {
				idx.Columns[j] = newCol
			}
		}
	}
}

func (t *Table) RemoveColumn(c *Column) {
	var idxToRemove []*Index
	for _, idx := range t.Indexes {
		for _, idxCol := range idx.Columns {
			if idxCol.ID == c.ID {
				idxToRemove = append(idxToRemove, idx)
				break
			}
		}
	}
	for _, idx := range idxToRemove {
		t.RemoveIndex(idx)
	}
	var pos int
	for i := range t.Columns {
		if t.Columns[i].ID == c.ID {
			pos = i
			break
		}
	}
	t.Columns = append(t.Columns[:pos], t.Columns[pos+1:]...)
	for i := range t.Values {
		t.Values[i] = append(t.Values[i][:pos], t.Values[i][pos+1:]...)
	}
}

func (t *Table) RenameColumn(c *Column, newName string) {
	oldName := c.Name
	c.Name = newName
	for _, idx := range t.Indexes {
		for _, idxCol := range idx.Columns {
			if idxCol.Name == oldName {
				idxCol.Name = newName
			}
		}
	}
}

func (t *Table) ReplaceColumn(oldCol, newCol *Column) {
	for _, idx := range t.Indexes {
		for i, idxCol := range idx.Columns {
			if idxCol.ID == oldCol.ID {
				idx.Columns[i] = newCol
				break
			}
		}
	}
	incompatibleTp := !newCol.Tp.SameTypeAs(oldCol.Tp)
	for colIdx := range t.Columns {
		if t.Columns[colIdx].ID != oldCol.ID {
			continue
		}
		t.Columns[colIdx] = newCol
		for rowIdx := range t.Values {
			if incompatibleTp {
				// TODO: support reasonable data change.
				t.Values[rowIdx][colIdx] = newCol.ZeroValue()
			}
		}
		break
	}
}

func (t *Table) MoveColumnToFirst(col *Column) {
	pos := t.columnOffset(col)
	Assert(pos != -1)
	t.moveColumnCommon(pos, 0)
}

func (t *Table) MoveColumnAfterColumn(c1, c2 *Column) {
	p1 := t.columnOffset(c1)
	p2 := t.columnOffset(c2)
	if p1 == p2 {
		return
	} else if p1 < p2 {
		t.moveColumnCommon(p1, p2)
	} else {
		t.moveColumnCommon(p1, p2+1)
	}
}

func (t *Table) moveColumnCommon(src, dest int) {
	get := func(i int) interface{} { return t.Columns[i] }
	set := func(i int, v interface{}) { t.Columns[i] = v.(*Column) }
	Move(src, dest, get, set)
	for _, row := range t.Values {
		get := func(i int) interface{} { return row[i] }
		set := func(i int, v interface{}) { row[i] = v.(string) }
		Move(src, dest, get, set)
	}
}

func (t *Table) columnOffset(col *Column) int {
	for i, c := range t.Columns {
		if c.ID == col.ID {
			return i
		}
	}
	return -1
}

func (t *Table) AppendIndex(idx *Index) {
	t.Indexes = append(t.Indexes, idx)
	if idx.Tp == IndexTypePrimary {
		for _, c := range idx.Columns {
			c.IsNotNull = true
		}
	}
}

func (t *Table) RemoveIndex(idx *Index) {
	var pos int
	for i := range t.Indexes {
		if t.Indexes[i].ID == idx.ID {
			pos = i
			break
		}
	}
	t.Indexes = append(t.Indexes[:pos], t.Indexes[pos+1:]...)
}

func (t *Table) AppendRow(row []string) {
	t.Values = append(t.Values, row)
}

func (i *Index) AppendColumn(col *Column, prefix int) {
	i.Columns = append(i.Columns, col)
	i.ColumnPrefix = append(i.ColumnPrefix, prefix)
}

func (i *Index) AppendColumnIfNotExists(cols ...*Column) {
	for _, c := range cols {
		found := false
		for _, idxCol := range i.Columns {
			if idxCol.ID == c.ID {
				found = true
				break
			}
		}
		if found {
			continue
		}
		i.Columns = append(i.Columns, c)
		i.ColumnPrefix = append(i.ColumnPrefix, 0)
	}
}

func (p *Prepare) AppendColumns(cols ...*Column) {
	for _, c := range cols {
		p.Args = append(p.Args, func() string {
			return c.RandomValue()
		})
	}
}
