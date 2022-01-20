package sqlgen

import (
	"strconv"
	"strings"
)

func (t *Table) String() string {
	var sb strings.Builder
	sb.WriteString("create table ")
	sb.WriteString(t.Name)
	sb.WriteString(" (")
	for i, c := range t.Columns {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(c.String())
	}
	if len(t.Indices) != 0 {
		sb.WriteString(", ")
		for i, idx := range t.Indices {
			if i != 0 {
				sb.WriteString(",")
			}
			sb.WriteString(idx.String())
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func (c *Column) String() string {
	var sb strings.Builder
	sb.WriteString(c.Name)
	sb.WriteString(" ")
	sb.WriteString(c.Tp.String())
	switch {
	case c.arg1 == 0 && c.arg2 == 0:
	case c.arg1 != 0 && c.arg2 == 0:
		sb.WriteString("(")
		sb.WriteString(strconv.Itoa(c.arg1))
		sb.WriteString(")")
	default:
		sb.WriteString("(")
		sb.WriteString(strconv.Itoa(c.arg1))
		sb.WriteString(",")
		sb.WriteString(strconv.Itoa(c.arg2))
		sb.WriteString(")")
	}
	return sb.String()
}

func (i *Index) String() string {
	var sb strings.Builder
	sb.WriteString(i.Tp.String())
	sb.WriteString(" ")
	sb.WriteString(i.Name)
	for i, c := range i.Columns {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(c.Name)
	}
	return sb.String()
}
