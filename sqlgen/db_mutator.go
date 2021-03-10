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

func (s *State) UpdateCtrlOption(fn func(option *ControlOption)) {
	fn(s.ctrl)
}

func (s *State) AppendTable(tbl *Table) {
	s.tables = append(s.tables, tbl)
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
	t.Columns = append(t.Columns, c)
	for i := range t.values {
		t.values[i] = append(t.values[i], c.ZeroValue())
	}
}

func (t *Table) AppendPartitionColumn(c *Column) {
	t.PartitionColumns = append(t.PartitionColumns, c)
}

func (t *Table) RemoveColumn(c *Column) {
	var pos int
	for i := range t.Columns {
		if t.Columns[i].Name == c.Name {
			pos = i
			break
		}
	}
	t.Columns = append(t.Columns[:pos], t.Columns[pos+1:]...)
	for i := range t.values {
		t.values[i] = append(t.values[i][:pos], t.values[i][pos+1:]...)
	}
}

// Only use it when there is no table data.
func (t *Table) ReorderColumns() {
	Assert(len(t.values) == 0, "ReorderColumns should only be used when there is no table data")
	sort.Slice(t.Columns, func(i, j int) bool {
		return t.Columns[i].Id < t.Columns[j].Id
	})
}

func (t *Table) SetPrimaryKeyAndHandle(state *State) {
	t.containsPK = true
	for _, idx := range t.Indices {
		if idx.Tp == IndexTypePrimary {
			if len(idx.Columns) == 1 && idx.Columns[0].Tp.IsIntegerType() {
				t.HandleCols = idx.Columns
				return
			}
			if state.enabledClustered {
				t.HandleCols = idx.Columns
				return
			}
			break
		}
	}
	t.HandleCols = []*Column{{
		Id:         0,
		Name:       "_tidb_rowid",
		Tp:         ColumnTypeBigInt,
		isNotNull:  true,
		isUnsigned: true,
	}}
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
			if idxCol.Id == c.Id {
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
