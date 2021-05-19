package sqlgen

import (
	"sort"
)

func (s *State) PopOneTodoSQL() (string, bool) {
	if len(s.todoSQLs) == 0 {
		return "", false
	}
	sql := s.todoSQLs[0]
	s.todoSQLs = s.todoSQLs[1:]
	return sql, true
}

func (s *State) InjectTodoSQL(sqls ...string) {
	s.todoSQLs = append(s.todoSQLs, sqls...)
}

func (s *State) SetWeight(prod Fn, weight int) {
	Assert(weight >= 0)
	s.weight[prod.Info] = weight
}

func (s *State) SetRepeat(prod Fn, lower int, upper int) {
	Assert(lower > 0)
	Assert(lower <= upper)
	s.repeat[prod.Info] = Interval{lower, upper}
}

func (s *State) UpdateCtrlOption(fn func(option *ControlOption)) {
	fn(s.ctrl)
}

func (s *State) AppendHook(hook FnEvaluateHook) {
	s.hooks = append(s.hooks, hook)
}

func (s *State) RemoveHook(hookInfo string) {
	filled := 0
	for _, h := range s.hooks {
		if h.Info() != hookInfo {
			s.hooks[filled] = h
			filled++
		}
	}
	s.hooks = s.hooks[:filled]
}

func (s *State) AppendTable(tbl *Table) {
	s.tables = append(s.tables, tbl)
}

func (s *State) PushCTE(cte *CTE) {
	s.ctes[len(s.ctes)-1] = append(s.ctes[len(s.ctes)-1], cte)
}

func (s *State) PopCTE() []*CTE {
	if len(s.ctes) == 0 {
		panic("0 CTEs")
	}
	tc := s.ctes[len(s.ctes)-1]
	s.ctes = s.ctes[:len(s.ctes)-1]
	return tc
}

func (s *State) CurrentCTE() *CTE {
	if len(s.ctes) == 0 {
		return nil
	}
	l := s.ctes[len(s.ctes)-1]
	if len(l) == 0 {
		return nil
	}
	return l[len(l)-1]
}

func (s *State) LastCTEs() []*CTE {
	return s.ctes[len(s.ctes)-1]
}

func (s *State) ParentCTEColCount() int {
	if len(s.ctes) < 2 {
		return 0
	}
	ctes := s.ctes[len(s.ctes)-2]
	return len(ctes[len(ctes)-1].Cols)
}

func (s *State) AppendPrepare(pre *Prepare) {
	s.prepareStmts = append(s.prepareStmts, pre)
}

func (s *State) RemovePrepare(p *Prepare) {
	var pos int
	for i := range s.prepareStmts {
		if s.prepareStmts[i].Id == p.Id {
			pos = i
			break
		}
	}
	s.prepareStmts = append(s.prepareStmts[:pos], s.prepareStmts[pos+1:]...)
}

func (t *Table) AppendColumn(c *Column) {
	c.relatedTableID = t.ID
	t.Columns = append(t.Columns, c)
	for i := range t.values {
		t.values[i] = append(t.values[i], c.ZeroValue())
	}
}

func (t *Table) RemoveColumn(c *Column) {
	var pos int
	for i := range t.Columns {
		if t.Columns[i].ID == c.ID {
			pos = i
			break
		}
	}
	t.Columns = append(t.Columns[:pos], t.Columns[pos+1:]...)
	for i := range t.values {
		t.values[i] = append(t.values[i][:pos], t.values[i][pos+1:]...)
	}
}

func (t *Table) ReplaceColumn(oldCol, newCol *Column) {
	newCol.relatedTableID = t.ID
	for colIdx := range t.Columns {
		if t.Columns[colIdx].ID != oldCol.ID {
			continue
		}
		t.Columns[colIdx] = newCol
		for rowIdx := range t.values {
			// TODO: support reasonable data change.
			t.values[rowIdx][colIdx] = newCol.ZeroValue()
		}
		break
	}
}

// Only use it when there is no table data.
func (t *Table) ReorderColumns() {
	Assert(len(t.values) == 0, "ReorderColumns should only be used when there is no table data")
	sort.Slice(t.Columns, func(i, j int) bool {
		return t.Columns[i].ID < t.Columns[j].ID
	})
}

func (t *Table) AppendIndex(idx *Index) {
	for _, idxCol := range idx.Columns {
		idxCol.relatedIndices[idx.Id] = struct{}{}
	}
	t.Indices = append(t.Indices, idx)
}

func (t *Table) RemoveIndex(idx *Index) {
	var pos int
	for i := range t.Indices {
		if t.Indices[i].Id == idx.Id {
			pos = i
			break
		}
	}
	t.Indices = append(t.Indices[:pos], t.Indices[pos+1:]...)
	for _, idxCol := range idx.Columns {
		delete(idxCol.relatedIndices, idx.Id)
	}
}

func (t *Table) AppendRow(row []string) {
	t.values = append(t.values, row)
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
		c.relatedIndices[i.Id] = struct{}{}
	}
}

func (p *Prepare) AppendColumns(cols ...*Column) {
	for _, c := range cols {
		p.Args = append(p.Args, func() string {
			return c.RandomValue()
		})
	}
}
