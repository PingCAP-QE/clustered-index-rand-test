package sqlgen

import (
	"fmt"
	"strconv"
	"strings"
)

func PrintTableNames(tbs []*Table) string {
	var sb strings.Builder
	for i, t := range tbs {
		sb.WriteString(t.Name)
		if i != len(tbs)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

func PrintQualifiedColumnNames(tableColPairs TableColumnPairs) string {
	var sb strings.Builder
	for i, pair := range tableColPairs {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(PrintQualifiedColumnName(pair.t, pair.c))
	}
	return sb.String()
}

func PrintQualifiedColumnName(tb *Table, col *Column) string {
	return fmt.Sprintf("%s.%s", tb.Name, col.Name)
}

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

// DECIMAL(10,3) / VARCHAR(255) COLLATE utf8_bin / INT UNSIGNED DEFAULT 0 / FLOAT(5, 3) NOT NULL...
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
	if c.Tp.IsStringType() {
		sb.WriteString(" collate ")
		sb.WriteString(c.collate.String())
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

// partition p0 values less than (v0), partition p1...
func PrintRangePartitionDefs(rangeValues []string) string {
	var sb strings.Builder
	for i, v := range rangeValues {
		sb.WriteString("partition ")
		sb.WriteString(fmt.Sprintf("p%d ", i))
		sb.WriteString("values less than (")
		sb.WriteString(v)
		sb.WriteString(")")
		if i != len(rangeValues)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

// partition p0 values in (v0, v1, v2), partition p1...
func PrintListPartitionDefs(listGroups [][]string) string {
	var sb strings.Builder
	for i, group := range listGroups {
		sb.WriteString("partition ")
		sb.WriteString(fmt.Sprintf("p%d ", i))
		sb.WriteString("values in (")
		sb.WriteString(PrintRandValues(group))
		sb.WriteString(")")
		if i != len(listGroups)-1 {
			sb.WriteString(", ")
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

func PrintRandomAssignments(cols []*Column) string {
	var sb strings.Builder
	for i, col := range cols {
		sb.WriteString(col.Name)
		sb.WriteString(" = ")
		sb.WriteString(col.RandomValue())
		if i != len(cols)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func PrintSplitByItems(rows [][]string) string {
	var sb strings.Builder
	for i, item := range rows {
		sb.WriteString("(")
		sb.WriteString(PrintRandValues(item))
		sb.WriteString(")")
		if i != len(rows)-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

// (col1, col2...) = (val11, val12...) or (col1, col2...) = (val21, val22...)...
func PrintPredicateCompoundDNF(cols []*Column, values [][]string) string {
	Assert(len(cols) > 0)
	var sb strings.Builder
	sb.WriteString("(")
	for i, c := range cols {
		sb.WriteString(c.Name)
		if i < len(cols)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	sb.WriteString(" = ")
	condPrefix := sb.String()
	sb.Reset()
	for i := 0; i < len(values); i++ {
		sb.WriteString(condPrefix)
		sb.WriteString("(")
		sb.WriteString(strings.Join(values[i], ", "))
		sb.WriteString(")")
		if i < len(values)-1 {
			sb.WriteString(" or ")
		}
	}
	return sb.String()
}

// (col1 = val11 and col2 = val12 and ...) or (col1 = val21 and col2 = val22 and ...) ...
func PrintPredicateDNF(cols []*Column, values [][]string) string {
	var sb strings.Builder
	for i, row := range values {
		sb.WriteString("(")
		for j, v := range row {
			sb.WriteString(cols[j].Name)
			sb.WriteString(" = ")
			sb.WriteString(v)
			if j < len(row)-1 {
				sb.WriteString(" and ")
			}
		}
		sb.WriteString(")")
		if i < len(values)-1 {
			sb.WriteString(" or ")
		}
	}
	return sb.String()
}

// (col1, col2...) in ((val11, val12...), (val21, val22...), ...)
func PrintPredicateIn(cols []*Column, values [][]string) string {
	var sb strings.Builder
	sb.WriteString("(")
	for i, c := range cols {
		sb.WriteString(c.Name)
		if i < len(cols)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	sb.WriteString(" in (")
	for i, row := range values {
		sb.WriteString("(")
		for j, v := range row {
			sb.WriteString(v)
			if j < len(row)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(")")
		if i < len(values)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func PrintColumnWithFunction(col *Column) string {
	switch col.Tp {
	case ColumnTypeInt:
		return fmt.Sprintf("%s+1", col.Name)
	case ColumnTypeChar:
		return fmt.Sprintf("concat(%s, 1)", col.Name)
	}

	return col.Name
}

func PrintConstantWithFunction(tp ColumnType) string {
	switch tp {
	case ColumnTypeInt:
		return fmt.Sprintf("1+1")
	case ColumnTypeChar:
		return fmt.Sprintf("concat('a', 1)")
	}

	return "1"
}
