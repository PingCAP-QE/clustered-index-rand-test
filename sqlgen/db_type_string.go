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
	if len(t.Indexes) != 0 {
		sb.WriteString(", ")
		for i, idx := range t.Indexes {
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
	case c.Arg1 == 0 && c.Arg2 == 0:
	case c.Arg1 != 0 && c.Arg2 == 0:
		sb.WriteString("(")
		sb.WriteString(strconv.Itoa(c.Arg1))
		sb.WriteString(")")
	default:
		sb.WriteString("(")
		sb.WriteString(strconv.Itoa(c.Arg1))
		sb.WriteString(",")
		sb.WriteString(strconv.Itoa(c.Arg2))
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
