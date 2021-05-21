package sqlgen

import (
	"fmt"
	"math/rand"
)

func (s *State) GetRandTable() *Table {
	return s.tables[rand.Intn(len(s.tables))]
}

func (s *State) GetAllTables() Tables {
	return s.tables
}

func (s *State) IncCTEDeep() {
	s.ctes = append(s.ctes, make([]*CTE, 0))
}

func (s *State) GetTableByID(id int) *Table {
	for _, t := range s.tables {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (s *State) GetRelatedTables(cols []*Column) []*Table {
	tbs := make([]*Table, len(cols))
	for i, c := range cols {
		tbs[i] = s.GetTableByID(c.relatedTableID)
		Assert(tbs[i] != nil)
	}
	return tbs
}

func (s *State) GetRandPrepare() *Prepare {
	return s.prepareStmts[rand.Intn(len(s.prepareStmts))]
}

func (s *State) FilterTables(pred func(t *Table) bool) Tables {
	ret := make(Tables, 0, len(s.tables)/2)
	for _, t := range s.tables {
		if pred(t) {
			ret = append(ret, t)
		}
	}
	return ret
}

type Tables []*Table

func (ts Tables) PickOne() *Table {
	return ts[rand.Intn(len(ts))]
}

func (ts Tables) One() *Table {
	if len(ts) > 1 {
		NeverReach()
	}
	return ts[0]
}

func (t *Table) GetRandColumn() *Column {
	return t.Columns[rand.Intn(len(t.Columns))]
}

// GetRandIndexFirstColumn returns a random index's first columns.
// If there is no index, return GetRandColumn().
func (t *Table) GetRandIndexFirstColumn() *Column {
	if len(t.Indices) == 0 {
		return t.GetRandColumn()
	}
	idx := t.Indices[rand.Intn(len(t.Indices))]
	return idx.Columns[0]
}

// GetRandIndexPrefixColumn returns a random index's random prefix columns.
func (t *Table) GetRandIndexPrefixColumn() []*Column {
	if len(t.Indices) == 0 {
		return nil
	}
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
	restCols := make([]*Column, 0, len(t.Columns)/2)
	for _, c := range t.Columns {
		if pred(c) {
			restCols = append(restCols, c)
		}
	}
	return restCols
}

func (t *Table) FilterIndexes(pred func(idx *Index) bool) []*Index {
	restIdx := make([]*Index, 0, len(t.Indices)/2)
	for _, i := range t.Indices {
		if pred(i) {
			restIdx = append(restIdx, i)
		}
	}
	return restIdx
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
			if tableCol.ID == targetCol.ID {
				vals = append(vals, randRow[i])
				break
			}
		}
	}
	return vals
}

func (t *Table) GetRandRows(cols []*Column, rowCount int) [][]string {
	if len(t.values) == 0 {
		return nil
	}
	rows := make([][]string, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = t.GetRandRow(cols)
	}
	return rows
}

func (t *Table) GetRandRowVal(col *Column) string {
	if len(t.values) == 0 {
		return ""
	}
	randRow := t.values[rand.Intn(len(t.values))]
	for i, c := range t.Columns {
		if c.ID == col.ID {
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
		newCol := *c
		newCol.ID = colIDFn()
		newCol.relatedIndices = map[int]struct{}{}
		newCol.relatedTableID = tblID
		oldID2NewCol[c.ID] = &newCol
		newCols = append(newCols, &newCol)
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
			newIdx.Columns = append(newIdx.Columns, oldID2NewCol[ic.ID])
			ic.relatedIndices[idxID] = struct{}{}
		}
		newIdxs = append(newIdxs, newIdx)
	}

	newTable := &Table{
		ID:         tblID,
		Name:       name,
		Columns:    newCols,
		Indices:    newIdxs,
		containsPK: t.containsPK,
		values:     nil,
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
	return t.GetRandColumnsNonEmpty()
}

func (t *Table) GetRandColumnsNonEmpty() []*Column {
	count := 1 + rand.Intn(len(t.Columns))
	return t.GetRandNColumns(count)
}

func (t *Table) GetRandNColumns(n int) []*Column {
	cols := t.cloneColumns()
	rand.Shuffle(len(cols), func(i, j int) {
		cols[i], cols[j] = cols[j], cols[i]
	})
	return cols[:n]
}

// GetRandUniqueIndexForPointGet gets a random unique index.
func (t *Table) GetRandUniqueIndexForPointGet() *Index {
	uniqueIdxs := t.FilterIndexes(func(idx *Index) bool {
		return idx.IsUnique() && idx.Columns[0].Tp.IsPointGetableType()
	})
	if len(uniqueIdxs) == 0 {
		return nil
	}
	return uniqueIdxs[rand.Intn(len(uniqueIdxs))]
}

// GetColumnOffset gets the offset for a column.
func (t *Table) GetColumnOffset(column *Column) int {
	for i, col := range t.Columns {
		if col.ID == column.ID {
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

func (t *Table) GetUniqueKeyColumns() []*Column {
	indexes := t.FilterIndexes(func(idx *Index) bool {
		return idx.Tp == IndexTypePrimary || idx.Tp == IndexTypeUnique
	})
	if len(indexes) == 0 {
		return nil
	}
	return indexes[rand.Intn(len(indexes))].Columns
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

func (c *Column) QualifiedName(state *State) string {
	var tableName string
	for _, t := range state.tables {
		if c.relatedTableID == t.ID {
			tableName = t.Name
			break
		}
	}
	return fmt.Sprintf("%s.%s", tableName, c.Name)
}

func (p *Prepare) UserVars() []string {
	userVars := make([]string, len(p.Args))
	for i := 0; i < len(p.Args); i++ {
		userVars[i] = fmt.Sprintf("@i%d", i)
	}
	return userVars
}

func (c *CTE) AppendColumn(col *Column) {
	c.Cols = append(c.Cols, col)
}
