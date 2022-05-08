package sqlgen

import (
	"fmt"
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
