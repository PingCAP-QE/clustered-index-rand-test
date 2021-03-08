package sqlgen

import (
	"fmt"
	"strconv"
	"strings"
)

func PrintColumnNamesWithPar(cols []*Column, emptyMarker string) string {
	result := PrintColumnNamesWithoutPar(cols, emptyMarker)
	if result == emptyMarker {
		return emptyMarker
	}
	return "(" + result + ")"
}

func PrintColumnNamesWithoutPar(cols []*Column, emptyMarker string) string {
	if len(cols) == 0 {
		return emptyMarker
	}
	var sb strings.Builder
	for i, c := range cols {
		sb.WriteString(c.Name)
		if i != len(cols)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func PrintRandValues(values []string) string {
	return strings.Join(values, ",")
}

func PrintColumnType(c *Column) string {
	var sb strings.Builder
	sb.WriteString(c.Tp.String())
	if c.arg1 != 0 {
		sb.WriteString("(")
		sb.WriteString(strconv.Itoa(c.arg1))
		if c.arg2 != 0 {
			sb.WriteString(",")
			sb.WriteString(strconv.Itoa(c.arg2))
		}
		sb.WriteString(")")
	} else if len(c.args) != 0 {
		sb.WriteString("(")
		for i, a := range c.args {
			sb.WriteString("'")
			sb.WriteString(a)
			sb.WriteString("'")
			if i != len(c.args)-1 {
				sb.WriteString(",")
			}
		}
		sb.WriteString(")")
	}
	if c.isUnsigned {
		sb.WriteString(" unsigned")
	}
	if c.defaultVal != "" {
		sb.WriteString(" default ")
		sb.WriteString(c.defaultVal)
	}
	if c.isNotNull {
		sb.WriteString(" not null")
	}
	return sb.String()
}

func PrintIndexColumnNames(idx *Index) string {
	var sb strings.Builder
	for i, col := range idx.Columns {
		sb.WriteString(col.Name)
		if idx.ColumnPrefix[i] != 0 {
			sb.WriteString("(")
			sb.WriteString(strconv.Itoa(idx.ColumnPrefix[i]))
			sb.WriteString(")")
		}
		if i != len(idx.Columns)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func PrintIndexType(idx *Index) string {
	switch idx.Tp {
	case IndexTypeNonUnique:
		return ""
	case IndexTypeUnique:
		return "unique"
	case IndexTypePrimary:
		return "primary"
	default:
		return "invalid"
	}
}

func PrintFullQualifiedColName(tbl *Table, cols []*Column) string {
	if len(cols) == 0 {
		return fmt.Sprintf("%s.*", tbl.Name)
	}
	var sb strings.Builder
	for i, col := range cols {
		sb.WriteString(tbl.Name)
		sb.WriteString(".")
		sb.WriteString(col.Name)
		if i != len(cols)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}
