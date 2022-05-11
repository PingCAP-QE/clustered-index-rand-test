package sqlgen

import "strings"

type Env struct {
	prev []*Elem
	*Elem
}

type Elem struct {
	Table      *Table
	Column     *Column
	PartColumn *Column
	IdxColumn  *Column
	Columns    Columns
	Index      *Index
	QState     *QueryState
	QColumns   QueryStateColumns
	FnInfo     string
	Bool       bool
	MultiObjs  *MultiObjs
}

func (e *Env) Enter() {
	if e.Elem == nil {
		e.Elem = &Elem{}
	}
	newElem := *e.Elem
	e.prev = append(e.prev, &newElem)
}

func (e *Env) Leave() {
	if len(e.prev) == 0 {
		return
	}
	last := e.prev[len(e.prev)-1]
	*e.Elem = *last
	e.prev = e.prev[:len(e.prev)-1]
}

func (e *Env) Clean() {
	e.Elem = &Elem{}
	e.prev = nil
}

func (e *Env) Depth() int {
	if e.prev == nil {
		return 0
	}
	return len(e.prev)
}

func (e *Env) Clone() *Env {
	newEnv := &Env{prev: make([]*Elem, 0, e.Depth())}
	newEnv.Elem = &Elem{}
	*newEnv.Elem = *e.Elem
	for _, oldE := range e.prev {
		elem := *oldE
		newEnv.prev = append(newEnv.prev, &elem)
	}
	return newEnv
}

func (e *Env) GetCurrentStack() string {
	var sb strings.Builder
	for i, prev := range e.prev {
		if i > 0 {
			sb.WriteString("-")
		}
		sb.WriteString("'")
		sb.WriteString(prev.FnInfo)
		sb.WriteString("'")
	}
	return sb.String()
}

func (e *Env) IsIn(fn Fn) bool {
	for _, prev := range e.prev {
		if fn.Info == prev.FnInfo {
			return true
		}
	}
	return fn.Info == e.FnInfo
}
