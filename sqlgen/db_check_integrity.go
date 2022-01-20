package sqlgen

import "reflect"

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
			Assert(t.Columns.ContainColumn(idxCol))
		}
	}
}

func (c *Column) CheckIntegrity() {
	Assert(c != nil)
	Assert(len(c.Name) != 0)
}

func (i *Index) CheckIntegrity() {
	Assert(len(i.Name) > 0)
	Assert(len(i.Columns) > 0)
}

func NotNil(object interface{}) {
	if object == nil {
		panic("param is not initialized")
	}
	value := reflect.ValueOf(object)
	kind := value.Kind()
	for _, k := range []reflect.Kind{
		reflect.Chan, reflect.Func,
		reflect.Interface, reflect.Map,
		reflect.Ptr, reflect.Slice} {
		if k == kind && value.IsNil() {
			panic("param is not initialized")
		}
	}
}
