package sqlgen

import (
	"fmt"
	"math/rand"

	"github.com/cznic/mathutil"
)

var IndexDefinitions = NewFn(func(state *State) Fn {
	return Repeat(IndexDefinition.R(0, 4), Str(","))
})

var IndexDefinition = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	newIdx := &Index{ID: state.alloc.AllocIndexID()}
	state.env.Index = newIdx
	// Example:
	//   unique key idx_1 (a, b, c)
	//   primary key (a(2), b(3), c)
	ret := And(
		IndexDefinitionType,
		IndexDefinitionName,
		IndexDefinitionColumns,
		IndexDefinitionClustered,
	).Eval(state)
	// It is possible that no column can be used to build an index.
	if len(newIdx.Columns) == 0 {
		return Empty
	}
	tbl.AppendIndex(newIdx)
	return Str(ret)
})

var IndexDefinitionType = NewFn(func(state *State) Fn {
	return Or(
		IndexDefinitionTypeUnique,
		IndexDefinitionTypeNonUnique,
		IndexDefinitionTypePrimary.P(NoPrimaryKey),
	)
})

var IndexDefinitionName = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Name = fmt.Sprintf("idx_%d", idx.ID)
	if idx.Tp == IndexTypePrimary {
		return Empty
	}
	return Str(idx.Name)
})

var IndexDefinitionColumns = NewFn(func(state *State) Fn {
	return And(Str("("), Repeat(IndexDefinitionColumn.R(1, 3), Str(",")), Str(")"))
})

var IndexDefinitionColumn = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	idx := state.env.Index
	partCol := state.env.PartColumn
	if partCol != nil && idx.IsUnique() && !idx.Columns.Contain(partCol) {
		state.env.IdxColumn = partCol
		return Or(
			IndexDefinitionColumnNoPrefix,
			IndexDefinitionColumnPrefix.P(IndexColumnPrefixable),
		)
	}
	// json column can't be used as index column.
	totalCols := tbl.Columns.Filter(func(c *Column) bool {
		return c.Tp != ColumnTypeJSON && !idx.HasColumn(c)
	})
	if len(totalCols) == 0 {
		return Empty
	}
	state.env.IdxColumn = totalCols.Rand()
	return IndexDefinitionColumnCheckLen
})

var IndexDefinitionColumnCheckLen = NewFn(func(state *State) Fn {
	idx := state.env.Index
	col := state.env.IdxColumn
	currentLength := 0
	for _, c := range idx.Columns {
		currentLength += c.EstimateSizeInBytes()
	}
	if currentLength+col.EstimateSizeInBytes() > DefaultKeySizeLimit {
		return Empty
	}
	return Or(
		IndexDefinitionColumnNoPrefix.P(IndexColumnCanHaveNoPrefix),
		IndexDefinitionColumnPrefix.P(IndexColumnPrefixable),
	)
})

var IndexDefinitionColumnNoPrefix = NewFn(func(state *State) Fn {
	idx := state.env.Index
	col := state.env.IdxColumn
	idx.AppendColumn(col, 0)
	return Str(col.Name)
}).P(func(state *State) bool {
	col := state.env.IdxColumn
	return !col.Tp.NeedKeyLength()
})

var IndexDefinitionColumnPrefix = NewFn(func(state *State) Fn {
	idx := state.env.Index
	col := state.env.IdxColumn
	maxLength := mathutil.Min(col.arg1, 5)
	if maxLength == 0 {
		maxLength = 5
	}
	prefix := 1 + rand.Intn(maxLength)
	idx.AppendColumn(col, prefix)
	return Strs(col.Name, "(", Num(prefix), ")")
})

var IndexDefinitionTypeUnique = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Tp = IndexTypeUnique
	return Str("unique key")
})

var IndexDefinitionTypeNonUnique = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Tp = IndexTypeNonUnique
	return Str("key")
})

var IndexDefinitionTypePrimary = NewFn(func(state *State) Fn {
	idx := state.env.Index
	idx.Tp = IndexTypePrimary
	return Str("primary key")
})

var IndexDefinitionClustered = NewFn(func(state *State) Fn {
	idx := state.env.Index
	if idx.Tp != IndexTypePrimary {
		return Empty
	}
	return Or(
		Str("/*T![clustered_index] clustered */"),
		Str("/*T![clustered_index] nonclustered */"),
	)
})
