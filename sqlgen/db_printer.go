package sqlgen

import (
	"fmt"
	"math/rand"
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

type aggArg struct {
	name        string
	minArg      int
	maxArg      int
	canDistinct bool
}

var aggFunctions = []aggArg{
	{"count", 1, 1, true},
	{"sum", 1, 1, true},
	{"avg", 1, 1, true},
	{"max", 1, 1, true},
	{"min", 1, 1, true},
	{"group_concat", 1, 1, true},
	{"bit_or", 1, 1, false},
	{"bit_xor", 1, 1, false},
	{"bit_and", 1, 1, false},
	{"var_pop", 1, 1, true},
	{"var_samp", 1, 1, true},
	{"stddev_pop", 1, 1, true},
	{"stddev_samp", 1, 1, true},
	{"json_objectagg", 2, 2, false},
	{"approx_count_distinct", 1, 1, false},
	{"approx_percentile", 1, 2, false},
}

func PrintRandomAggFunc(tbl *Table, cols []*Column) string {
	arg := aggFunctions[rand.Intn(len(aggFunctions))]
	str := arg.name + "("
	if arg.canDistinct && rand.Intn(3) == 0 {
		str += "distinct "
	}
	for i := 0; i < arg.minArg; i++ {
		col := cols[rand.Intn(len(cols))]
		str += col.Name
		if i > 0 {
			str += ","
		}
	}
	if arg.name == "approx_percentile" {
		str += ", " + strconv.FormatInt(rand.Int63n(100), 10)
	}
	str += ") aggCol"
	return str
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
