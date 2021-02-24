package sqlgen

import (
	"fmt"
	"math/rand"
	"os"
)

func (s *State) IsInitializing() bool {
	if s.finishInit {
		return false
	}
	if len(s.tables) < s.ctrl.InitTableCount {
		return true
	}
	for _, t := range s.tables {
		if len(t.values) < s.ctrl.InitRowCount {
			return true
		}
	}
	_ = os.RemoveAll(SelectOutFileDir)
	_ = os.Mkdir(SelectOutFileDir, 0644)
	s.finishInit = true
	return false
}

func (s *State) GetRandTable() *Table {
	return s.tables[rand.Intn(len(s.tables))]
}

func (s *State) GetFirstNonFullTable() *Table {
	for _, t := range s.tables {
		if len(t.values) < s.ctrl.InitRowCount {
			return t
		}
	}
	return nil
}

func (s *State) GetRandPrepare() *Prepare {
	return s.prepareStmts[rand.Intn(len(s.prepareStmts))]
}

func (t *Table) GetRandColumn() *Column {
	return t.columns[rand.Intn(len(t.columns))]
}

func (t *Table) GetRandColumnForPartition() *Column {
	cols := t.FilterColumns(func(column *Column) bool {
		return column.tp.IsPartitionType()
	})
	if len(cols) == 0 {
		return nil
	}
	return cols[rand.Intn(len(cols))]
}

func (t *Table) GetRandDroppableColumn() *Column {
	restCols := t.FilterColumns(func(c *Column) bool {
		return c.IsDroppable()
	})
	return restCols[rand.Intn(len(restCols))]
}

func (t *Table) GetRandColumnsIncludedDefaultValue() []*Column {
	if RandomBool() {
		// insert into t values (...)
		return nil
	}
	// insert into t (cols..) values (...)
	totalCols := t.FilterColumns(func(c *Column) bool { return c.defaultVal != "" })
	selectedCols := t.FilterColumns(func(c *Column) bool { return c.defaultVal == "" })
	for len(totalCols) > 0 && RandomBool() {
		chosenIdx := rand.Intn(len(totalCols))
		chosenCol := totalCols[chosenIdx]
		totalCols[0], totalCols[chosenIdx] = totalCols[chosenIdx], totalCols[0]
		totalCols = totalCols[1:]

		selectedCols = append(selectedCols, chosenCol)
	}
	return selectedCols
}

func (t *Table) HasDroppableColumn() bool {
	for _, c := range t.columns {
		if c.IsDroppable() {
			return true
		}
	}
	return false
}

func (t *Table) FilterColumns(pred func(column *Column) bool) []*Column {
	restCols := make([]*Column, 0, len(t.columns))
	for _, c := range t.columns {
		if pred(c) {
			restCols = append(restCols, c)
		}
	}
	return restCols
}

func (t *Table) GetRandomIndex() *Index {
	return t.indices[rand.Intn(len(t.indices))]
}

func (t *Table) GetRandIntColumn() *Column {
	for _, c := range t.columns {
		if c.tp.IsIntegerType() {
			return c
		}
	}
	return nil
}

func (t *Table) GetRandRow(cols []*Column) []string {
	if len(t.values) == 0 {
		return nil
	}
	if len(cols) == 0 {
		return t.values[rand.Intn(len(t.values))]
	}
	vals := make([]string, 0, len(cols))
	randRow := t.values[rand.Intn(len(t.values))]
	for _, targetCol := range cols {
		for i, tableCol := range t.columns {
			if tableCol.id == targetCol.id {
				vals = append(vals, randRow[i])
				break
			}
		}
	}
	return vals
}

func (t *Table) GetRandRowVal(col *Column) string {
	if len(t.values) == 0 {
		return ""
	}
	randRow := t.values[rand.Intn(len(t.values))]
	for i, c := range t.columns {
		if c.id == col.id {
			return randRow[i]
		}
	}
	return "GetRandRowVal: column not found"
}

func (t *Table) GetHandleColumns() []*Column {
	return t.handleCols
}

func (t *Table) cloneColumns() []*Column {
	cols := make([]*Column, len(t.columns))
	for i, c := range t.columns {
		cols[i] = c
	}
	return cols
}

func (t *Table) Clone(tblIDFn, colIDFn, idxIDFn func() int) *Table {
	tblID := tblIDFn()
	name := fmt.Sprintf("tbl_%d", tblID)

	oldID2NewCol := make(map[int]*Column, len(t.columns))
	newCols := make([]*Column, 0, len(t.columns))
	for _, c := range t.columns {
		colID := colIDFn()
		colName := fmt.Sprintf("col_%d", colID)
		newCol := &Column{
			id:             colID,
			name:           colName,
			tp:             c.tp,
			isUnsigned:     c.isUnsigned,
			arg1:           c.arg1,
			arg2:           c.arg2,
			args:           c.args,
			defaultVal:     c.defaultVal,
			isNotNull:      c.isNotNull,
			relatedIndices: map[int]struct{}{},
		}
		oldID2NewCol[c.id] = newCol
		newCols = append(newCols, newCol)
	}
	newIdxs := make([]*Index, 0, len(t.indices))
	for _, idx := range t.indices {
		idxID := idxIDFn()
		idxName := fmt.Sprintf("idx_%d", idxID)
		newIdx := &Index{
			id:           idxID,
			name:         idxName,
			tp:           idx.tp,
			columnPrefix: idx.columnPrefix,
		}
		newIdx.columns = make([]*Column, 0, len(idx.columns))
		for _, ic := range idx.columns {
			newIdx.columns = append(newIdx.columns, oldID2NewCol[ic.id])
			ic.relatedIndices[idxID] = struct{}{}
		}
		newIdxs = append(newIdxs, newIdx)
	}
	newHandleCols := make([]*Column, 0, len(t.handleCols))
	for _, oldHdCol := range t.handleCols {
		newHandleCols = append(newHandleCols, oldID2NewCol[oldHdCol.id])
	}
	Assert(len(newHandleCols) > 0, oldID2NewCol)
	newPartitionCols := make([]*Column, 0, len(t.partitionColumns))
	for _, oldPartCol := range t.partitionColumns {
		newPartitionCols = append(newPartitionCols, oldID2NewCol[oldPartCol.id])
	}

	newTable := &Table{
		id:               tblID,
		name:             name,
		columns:          newCols,
		indices:          newIdxs,
		containsPK:       t.containsPK,
		handleCols:       newHandleCols,
		partitionColumns: newPartitionCols,
		values:           nil,
	}
	newTable.childTables = []*Table{newTable}
	// TODO: DROP TABLE need to remove itself from children tables.
	t.childTables = append(t.childTables, newTable)
	return newTable
}

func (t *Table) GetRandColumns() []*Column {
	if RandomBool() {
		// insert into t values (...)
		return nil
	}
	// insert into t (cols..) values (...)
	totalCols := t.cloneColumns()
	var selectedCols []*Column
	for {
		chosenIdx := rand.Intn(len(totalCols))
		chosenCol := totalCols[chosenIdx]
		totalCols[0], totalCols[chosenIdx] = totalCols[chosenIdx], totalCols[0]
		totalCols = totalCols[1:]

		selectedCols = append(selectedCols, chosenCol)
		if len(totalCols) == 0 || RandomBool() {
			break
		}
	}
	return selectedCols
}

func (i *Index) IsUnique() bool {
	return i.tp == IndexTypePrimary || i.tp == IndexTypeUnique
}

func (c *Column) IsDroppable() bool {
	return len(c.relatedIndices) == 0
}

func (p *Prepare) UserVars() []string {
	userVars := make([]string, len(p.args))
	for i := 0; i < len(p.args); i++ {
		userVars[i] = fmt.Sprintf("@i%d", i)
	}
	return userVars
}
