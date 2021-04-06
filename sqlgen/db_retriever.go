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
	return t.Columns[rand.Intn(len(t.Columns))]
}

// GetRandIndexFirstColumnWithWeight returns a random index's first columns.
// It will choose PK according to the probability of pkW / (pkW + npkW).
// If there is no index, return GetRandColumn().
func (t *Table) GetRandIndexFirstColumnWithWeight(pkW, npkW int) *Column {
	if len(t.Indices) == 0 {
		return t.GetRandColumn()
	}
	var pk *Index
	npks := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		if idx.Tp == IndexTypePrimary {
			pk = idx
		} else {
			npks = append(npks, idx)
		}
	}

	if rand.Intn(pkW+npkW) < pkW {
		if pk == nil {
			return t.GetRandColumn()
		}
		return pk.Columns[0]
	} else {
		if len(npks) == 0 {
			return t.GetRandColumn()
		}
		return npks[rand.Intn(len(npks))].Columns[0]
	}
}

// GetRandIndexPrefixColumn returns a random index's random prefix columns.
func (t *Table) GetRandIndexPrefixColumn() []*Column {
	idx := t.Indices[rand.Intn(len(t.Indices))]
	randIdx := rand.Intn(len(idx.Columns))
	for i, idxCol := range idx.Columns {
		if idxCol.Tp == ColumnTypeBit || idxCol.Tp == ColumnTypeSet || idxCol.Tp == ColumnTypeEnum {
			randIdx = i - 1
			break
		}
	}

	return idx.Columns[0 : randIdx+1]
}

func (t *Table) GetRandColumnForPartition() *Column {
	cols := t.FilterColumns(func(column *Column) bool {
		return column.Tp.IsPartitionType()
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
	for _, c := range t.Columns {
		if c.IsDroppable() {
			return true
		}
	}
	return false
}

func (t *Table) FilterColumns(pred func(column *Column) bool) []*Column {
	restCols := make([]*Column, 0, len(t.Columns))
	for _, c := range t.Columns {
		if pred(c) {
			restCols = append(restCols, c)
		}
	}
	return restCols
}

func (t *Table) GetRandomIndex() *Index {
	return t.Indices[rand.Intn(len(t.Indices))]
}

func (t *Table) GetRandIntColumn() *Column {
	for _, c := range t.Columns {
		if c.Tp.IsIntegerType() {
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
		for i, tableCol := range t.Columns {
			if tableCol.Id == targetCol.Id {
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
	for i, c := range t.Columns {
		if c.Id == col.Id {
			return randRow[i]
		}
	}
	return "GetRandRowVal: column not found"
}

func (t *Table) cloneColumns() []*Column {
	cols := make([]*Column, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = c
	}
	return cols
}

func (t *Table) Clone(tblIDFn, colIDFn, idxIDFn func() int) *Table {
	tblID := tblIDFn()
	name := fmt.Sprintf("tbl_%d", tblID)

	oldID2NewCol := make(map[int]*Column, len(t.Columns))
	newCols := make([]*Column, 0, len(t.Columns))
	for _, c := range t.Columns {
		colID := colIDFn()
		newCol := &Column{
			Id:             colID,
			Name:           c.Name,
			Tp:             c.Tp,
			isUnsigned:     c.isUnsigned,
			arg1:           c.arg1,
			arg2:           c.arg2,
			args:           c.args,
			defaultVal:     c.defaultVal,
			isNotNull:      c.isNotNull,
			relatedIndices: map[int]struct{}{},
		}
		oldID2NewCol[c.Id] = newCol
		newCols = append(newCols, newCol)
	}
	newIdxs := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		idxID := idxIDFn()
		newIdx := &Index{
			Id:           idxID,
			Name:         idx.Name,
			Tp:           idx.Tp,
			ColumnPrefix: idx.ColumnPrefix,
		}
		newIdx.Columns = make([]*Column, 0, len(idx.Columns))
		for _, ic := range idx.Columns {
			newIdx.Columns = append(newIdx.Columns, oldID2NewCol[ic.Id])
			ic.relatedIndices[idxID] = struct{}{}
		}
		newIdxs = append(newIdxs, newIdx)
	}
	newPartitionCols := make([]*Column, 0, len(t.PartitionColumns))
	for _, oldPartCol := range t.PartitionColumns {
		newPartitionCols = append(newPartitionCols, oldID2NewCol[oldPartCol.Id])
	}

	newTable := &Table{
		Id:               tblID,
		Name:             name,
		Columns:          newCols,
		Indices:          newIdxs,
		containsPK:       t.containsPK,
		PartitionColumns: newPartitionCols,
		values:           nil,
	}
	newTable.childTables = []*Table{newTable}
	// TODO: DROP TABLE need to remove itself from children tables.
	t.childTables = append(t.childTables, newTable)
	return newTable
}

func (t *Table) GetAllColFns() []Fn {
	result := make([]Fn, 0, len(t.Columns))
	cols := t.cloneColumns()
	for _, col := range cols {
		result = append(result, Str(col.Name))
	}
	return result
}

func (t *Table) GetRandColumns() []*Column {
	if RandomBool() {
		// insert into t values (...)
		return nil
	}
	// insert into t (cols..) values (...)
	return t.GetRandColumnsNonEmpty()
}

func (t *Table) GetRandColumnsNonEmpty() []*Column {
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

// GetRandUniqueIndexForPointGet gets a random unique index.
func (t *Table) GetRandUniqueIndexForPointGet() *Index {
	idxs := make([]*Index, 0)
	for _, idx := range t.Indices {
		if idx.IsUnique() {
			for _, col := range idx.Columns {
				if col.Tp == ColumnTypeFloat || col.Tp == ColumnTypeDouble || col.Tp == ColumnTypeText || col.Tp == ColumnTypeBlob {
					continue
				}
			}
			idxs = append(idxs, idx)
		}
	}

	if len(idxs) == 0 {
		return nil
	}

	return idxs[rand.Intn(len(idxs))]
}

// GetColumnOffset gets the offset for a column.
func (t *Table) GetColumnOffset(column *Column) int {
	for i, col := range t.Columns {
		if col.Id == column.Id {
			return i
		}
	}
	Assert(false)
	return 0
}

// GetRandColumnsPreferIndex gets a random column, and give the indexed column more chance.
func (t *Table) GetRandColumnsPreferIndex() *Column {
	var col *Column
	for i := 0; i <= 5; i++ {
		col = t.Columns[rand.Intn(len(t.Columns))]
		if len(col.relatedIndices) > 0 {
			return col
		}
	}
	return col
}

func (t *Table) GetPrimaryKeyIndex() *Index {
	for _, idx := range t.Indices {
		if idx.Tp == IndexTypePrimary {
			return idx
		}
	}
	return nil
}

// GetRandColumnsSimple gets a random column.
func (t *Table) GetRandColumnsSimple() *Column {
	return t.Columns[rand.Intn(len(t.Columns))]
}

func (i *Index) IsUnique() bool {
	return i.Tp == IndexTypePrimary || i.Tp == IndexTypeUnique
}

func (i *Index) HasDefaultNullColumn() bool {
	for _, c := range i.Columns {
		if c.defaultVal == "null" {
			return true
		}
	}
	return false
}

func (c *Column) IsDroppable() bool {
	return len(c.relatedIndices) == 0
}

func (p *Prepare) UserVars() []string {
	userVars := make([]string, len(p.Args))
	for i := 0; i < len(p.Args); i++ {
		userVars[i] = fmt.Sprintf("@i%d", i)
	}
	return userVars
}
