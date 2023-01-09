package sqlgen

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/cznic/mathutil"
)

type Tables []*Table

func (ts Tables) Rand() *Table {
	return gRand1(ts)
}

func (ts Tables) RandN(n int) Tables {
	return gRandN(ts, n)
}

func (ts Tables) ByID(id int) *Table {
	for _, t := range ts {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (ts Tables) Filter(pred func(*Table) bool) Tables {
	return gFilter(ts, pred)
}

func (ts Tables) Removed(tbl *Table) Tables {
	tmp := ts[:0]
	for _, t := range ts {
		if t != tbl {
			tmp = append(tmp, t)
		}
	}
	return tmp
}

func (ts Tables) Append(t *Table) Tables {
	return append(ts, t)
}

type Columns []*Column

func (s *State) GetRandTableOrCTE() *Table {
	return s.GetRandTableOrCTEs()[0]
}

func (s *State) GetRandTableOrCTEs() Tables {
	tbls := make([]*Table, 0)
	for _, t := range s.Tables {
		tbls = append(tbls, t)
	}
	for _, cte := range s.ctes {
		for _, c := range cte {
			tbls = append(tbls, c)
		}
	}

	rand.Shuffle(len(tbls), func(i, j int) {
		tbls[i], tbls[j] = tbls[j], tbls[i]
	})
	tbls = tbls[:mathutil.Min(10, len(tbls))]
	n := len(tbls)
	x := rand.Intn(n * n * (n + 1) * (n + 1) / 4)
	return tbls[:n-(int(math.Sqrt(2*math.Sqrt(float64(x))+0.25)-0.5))]
}

func (s *State) IncCTEDeep() {
	s.ctes = append(s.ctes, make([]*Table, 0))
}

func (s *State) GetRandPrepare() *Prepare {
	return s.prepareStmts[rand.Intn(len(s.prepareStmts))]
}

func (ts Tables) Copy() Tables {
	newTables := make(Tables, len(ts))
	for i := range ts {
		newTables[i] = ts[i]
	}
	return newTables
}

type Indexes []*Index

func (t *Table) GetRandRow(cols []*Column) []string {
	if len(t.Values) == 0 {
		return nil
	}
	if len(cols) == 0 {
		return t.Values[rand.Intn(len(t.Values))]
	}
	vals := make([]string, 0, len(cols))
	randRow := t.Values[rand.Intn(len(t.Values))]
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
	if len(t.Values) == 0 {
		return nil
	}
	rows := make([][]string, rowCount)
	for i := 0; i < rowCount; i++ {
		rows[i] = t.GetRandRow(cols)
	}
	return rows
}

func (t *Table) GetRandRowVal(col *Column) string {
	if len(t.Values) == 0 {
		return ""
	}
	randRow := t.Values[rand.Intn(len(t.Values))]
	for i, c := range t.Columns {
		if c.ID == col.ID {
			return randRow[i]
		}
	}
	return "GetRandRowVal: column not found"
}

func (t *Table) CloneCreateTableLike(state *State) *Table {
	newTable := t.Clone()
	newTable.ID = state.alloc.AllocTableID()
	newTable.Name = fmt.Sprintf("tbl_%d", newTable.ID)
	for _, c := range newTable.Columns {
		c.ID = state.alloc.AllocColumnID()
	}
	for _, idx := range newTable.Indexes {
		idx.ID = state.alloc.AllocIndexID()
	}
	newTable.Values = nil
	newTable.ColForPrefixIndex = nil
	return newTable
}

func (cols Columns) Filter(pred func(c *Column) bool) Columns {
	return gFilter(cols, pred)
}

func (cols Columns) Found(pred func(c *Column) bool) bool {
	return gExist(cols, pred)
}

func (cols Columns) Rand() *Column {
	return gRand1(cols)
}

func (cols Columns) RandN() Columns {
	if len(cols) == 0 {
		return nil
	}
	return gRandN(cols, rand.Intn(len(cols)))
}

func (cols Columns) RandNNotNil() Columns {
	if len(cols) == 0 {
		return nil
	}
	cnt := len(cols) - 1
	return gRandN(cols, 1+cnt)
}

func (cols Columns) Contain(c *Column) bool {
	return gContain(cols, c)
}

func (cols Columns) Concat(other Columns) Columns {
	return gConcat(cols, other)
}

func (cols Columns) Diff(other Columns) Columns {
	return gDiff(cols, other)
}

func (cols Columns) Copy() Columns {
	return gCopy(cols)
}

func (cols Columns) Equal(other Columns) bool {
	return gEqual(cols, other)
}

func (cols Columns) Span(pred func(column *Column) bool) (Columns, Columns) {
	return gSpan(cols, pred)
}

func (cols Columns) EstimateSizeInBytes() int {
	total := 0
	for _, c := range cols {
		total += c.EstimateSizeInBytes()
	}
	return total
}

func (cols Columns) ByID(id int) int {
	for i, c := range cols {
		if c.ID == id {
			return i
		}
	}
	return -1
}

func (cols Columns) Or(others Columns) Columns {
	if len(cols) == 0 {
		return others
	}
	return cols
}

func (c *Column) HasIndex(t *Table) bool {
	for _, idx := range t.Indexes {
		if idx.HasColumn(c) {
			return true
		}
	}
	return false
}

func (is Indexes) Found(pred func(index *Index) bool) bool {
	return gExist(is, pred)
}

func (is Indexes) Rand() *Index {
	return gRand1(is)
}

func (is Indexes) Primary() *Index {
	idx := is.Filter(func(index *Index) bool {
		return index.Tp == IndexTypePrimary
	})
	if len(idx) == 0 {
		return nil
	}
	return idx[0]
}

func (is Indexes) Filter(pred func(*Index) bool) Indexes {
	return gFilter(is, pred)
}

func (i *Index) IsUnique() bool {
	return i.Tp == IndexTypePrimary || i.Tp == IndexTypeUnique
}

func (i *Index) HasColumn(c *Column) bool {
	for _, idxCol := range i.Columns {
		if idxCol.ID == c.ID {
			return true
		}
	}
	return false
}

func (p *Prepare) UserVars() []string {
	userVars := make([]string, len(p.Args))
	for i := 0; i < len(p.Args); i++ {
		userVars[i] = fmt.Sprintf("@i%d", i)
	}
	return userVars
}
