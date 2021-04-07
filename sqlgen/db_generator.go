package sqlgen

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/cznic/mathutil"
)

func GenNewTable(id int) *Table {
	tblName := fmt.Sprintf("tbl_%d", id)
	newTbl := &Table{Id: id, Name: tblName}
	newTbl.childTables = []*Table{newTbl}
	return newTbl
}

func GenNewColumn(id int, w *Weight) *Column {
	col := &Column{Id: id, Name: fmt.Sprintf("col_%d", id)}
	col.Tp = ColumnType(rand.Intn(int(ColumnTypeMax)))
	// collate is only used if the type is string.
	col.collate = CollationType(rand.Intn(int(CollationTypeMax)-1) + 1)
	if w.CreateTable_MustStrCol {
		col.Tp = ColumnTypeChar + ColumnType(rand.Intn(int(3)))
	}
	if w.CreateTable_MustIntCol {
		col.Tp = ColumnTypeInt + ColumnType(rand.Intn(int(5)))
	}
	switch col.Tp {
	// https://docs.pingcap.com/tidb/stable/data-type-numeric
	case ColumnTypeFloat, ColumnTypeDouble:
		// Float/Double precision is deprecated.
		//https://github.com/pingcap/tidb/issues/21692
		col.arg1 = 0
		col.arg2 = 0
	case ColumnTypeDecimal:
		col.arg1 = 1 + rand.Intn(65)
		upper := mathutil.Min(col.arg1, 30)
		col.arg2 = 1 + rand.Intn(upper)
	case ColumnTypeBit:
		col.arg1 = 1 + rand.Intn(62)
	case ColumnTypeChar, ColumnTypeBinary:
		col.arg1 = 1 + rand.Intn(255)
	case ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeVarBinary:
		col.arg1 = 1 + rand.Intn(512)
	case ColumnTypeEnum, ColumnTypeSet:
		col.args = []string{"Alice", "Bob", "Charlie", "David"}
	}
	if col.Tp != ColumnTypeVarchar && rand.Intn(5) == 0 {
		col.arg1, col.arg2 = 0, 0
	}
	// Adjust collation. Set binary.
	if col.Tp == ColumnTypeBinary || col.Tp == ColumnTypeBlob || col.Tp == ColumnTypeVarBinary {
		col.collate = CollationBinary
	}
	if col.Tp.IsIntegerType() {
		col.isUnsigned = RandomBool()
	}
	col.isNotNull = RandomBool()
	if !col.Tp.DisallowDefaultValue() && RandomBool() {
		col.defaultVal = col.RandomValue()
	}
	col.relatedIndices = map[int]struct{}{}
	return col
}

func GenNewIndex(id int, tbl *Table, w *Weight) *Index {
	idx := &Index{Id: id, Name: fmt.Sprintf("idx_%d", id)}
	totalCols := tbl.cloneColumns()
	try := 10
	for {
		chosenIdx := rand.Intn(len(totalCols))
		chosenCol := totalCols[chosenIdx]
		if w.Query_INDEX_MERGE {
			// Make sure the first column is index-able.
			if (chosenCol.Tp == ColumnTypeBit || chosenCol.Tp == ColumnTypeSet || chosenCol.Tp == ColumnTypeEnum) && len(idx.Columns) == 0 && try != 0 {
				try--
				continue
			}
		}
		totalCols[0], totalCols[chosenIdx] = totalCols[chosenIdx], totalCols[0]
		totalCols = totalCols[1:]

		idx.Columns = append(idx.Columns, chosenCol)
		prefixLen := 0
		if chosenCol.Tp.NeedKeyLength() ||
			(chosenCol.Tp.IsStringType() && (w.CreateTable_MustPrefixIndex || rand.Intn(4) == 0)) {
			maxPrefix := mathutil.Min(chosenCol.arg1, 5)
			prefixLen = 1 + rand.Intn(maxPrefix+1)
		}
		idx.ColumnPrefix = append(idx.ColumnPrefix, prefixLen)
		keySizeInBytes := 0
		for _, c := range idx.Columns {
			keySizeInBytes += c.EstimateSizeInBytes()
		}
		if keySizeInBytes > DefaultKeySize {
			Assert(len(idx.Columns) > 1, keySizeInBytes, idx.Columns, "one column key size should not > 3072")
			idx.Columns = idx.Columns[:len(idx.Columns)-1]
			idx.ColumnPrefix = idx.ColumnPrefix[:len(idx.ColumnPrefix)-1]
			break
		}
		if len(totalCols) == 0 || RandomBool() {
			break
		}
	}
	if !tbl.containsPK && !idx.HasDefaultNullColumn() {
		tbl.containsPK = true
		idx.Tp = IndexTypePrimary
	} else {
		// Exclude primary key type.
		idx.Tp = IndexType(rand.Intn(int(IndexTypePrimary)))
	}
	return idx
}

func GenNewPrepare(id int) *Prepare {
	return &Prepare{
		Id:   id,
		Name: fmt.Sprintf("prepare_%d", id),
		Args: nil,
	}
}

func (t *Table) GenRandValues(cols []*Column) []string {
	if len(cols) == 0 {
		cols = t.Columns
	}
	row := make([]string, len(cols))
	for i, c := range cols {
		row[i] = c.RandomValue()
	}
	return row
}

// GenMultipleRowsAscForHandleCols generates random values for *possible* handle columns.
// It may be a random int64 or primary key columns' random values, because
// the generator have no idea about whether the primary key is clustered or not.
func (t *Table) GenMultipleRowsAscForHandleCols(count int) [][]string {
	rows := make([][]string, count)
	pkIdx := t.GetPrimaryKeyIndex()
	chooseClustered := RandomBool() && pkIdx != nil
	if chooseClustered {
		firstColumn := pkIdx.Columns[0].RandomValuesAsc(count)
		for i := 0; i < count; i++ {
			rows[i] = make([]string, len(pkIdx.Columns))
			for j := 0; j < len(pkIdx.Columns); j++ {
				if j == 0 {
					rows[i][j] = firstColumn[i]
				} else {
					rows[i][j] = pkIdx.Columns[j].RandomValue()
				}
			}
		}
		return rows
	}
	handles := RandomNums(0, 9223372036854775806, count)
	for i := 0; i < count; i++ {
		rows[i] = []string{handles[i]}
	}
	return rows
}

func (t *Table) GenMultipleRowsAscForIndexCols(count int, idx *Index) [][]string {
	rows := make([][]string, count)
	firstColumn := idx.Columns[0].RandomValuesAsc(count)
	for i := 0; i < count; i++ {
		rows[i] = make([]string, len(idx.Columns))
		for j := 0; j < len(idx.Columns); j++ {
			if j == 0 {
				rows[i][j] = firstColumn[i]
			} else {
				rows[i][j] = idx.Columns[j].RandomValue()
			}
		}
	}
	return rows
}

func (p *Prepare) GenAssignments() []string {
	todoSQLs := make([]string, len(p.Args))
	for i := 0; i < len(todoSQLs); i++ {
		todoSQLs[i] = fmt.Sprintf("set @i%d = %s", i, p.Args[i]())
	}
	return todoSQLs
}

func (c *Column) ZeroValue() string {
	switch c.Tp {
	case ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt, ColumnTypeBigInt, ColumnTypeBoolean:
		return "0"
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal, ColumnTypeBit:
		return "0"
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeBinary, ColumnTypeVarBinary:
		return "''"
	case ColumnTypeEnum, ColumnTypeSet:
		return fmt.Sprintf("'%s'", c.args[0])
	case ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp:
		return fmt.Sprintf("'2000-01-01'")
	case ColumnTypeTime:
		return fmt.Sprintf("'00:00:00'")
	default:
		return "invalid data type"
	}
}

func (c *Column) RandomValue() string {
	if !c.isNotNull && rand.Intn(30) == 0 {
		return "null"
	}
	return c.RandomValuesAsc(1)[0]
}

func (c *Column) RandomValueRange() (string, string) {
	values := c.RandomValuesAsc(2)
	return values[0], values[1]
}

func (c *Column) RandomValuesAsc(count int) []string {
	if count == 0 {
		return nil
	}
	if c.isUnsigned {
		switch c.Tp {
		case ColumnTypeTinyInt:
			return RandomNums(0, 255, count)
		case ColumnTypeSmallInt:
			return RandomNums(0, 65535, count)
		case ColumnTypeMediumInt:
			return RandomNums(0, 16777215, count)
		case ColumnTypeInt:
			return RandomNums(0, 4294967295, count)
		case ColumnTypeBigInt:
			return RandomNums(0, 9223372036854775806, count)
		}
	}
	switch c.Tp {
	case ColumnTypeTinyInt:
		return RandomNums(-128, 127, count)
	case ColumnTypeSmallInt:
		return RandomNums(-32768, 32767, count)
	case ColumnTypeMediumInt:
		return RandomNums(-8388608, 8388607, count)
	case ColumnTypeInt:
		return RandomNums(-2147483648, 2147483647, count)
	case ColumnTypeBigInt:
		return RandBigInts(count)
	case ColumnTypeBoolean:
		return RandomNums(0, 1, count)
	case ColumnTypeFloat, ColumnTypeDouble:
		return RandFloats(c.arg1, c.arg2, count)
	case ColumnTypeDecimal:
		m, d := c.arg1, c.arg2
		if m == 0 && d == 0 {
			m = 10
		}
		return RandFloats(m, d, count)
	case ColumnTypeBit:
		return RandomNums(0, (1<<c.arg1)-1, count)
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeBinary, ColumnTypeVarBinary:
		length := c.arg1
		if length == 0 {
			length = 1
		} else if length > 20 {
			length = 20
		}
		return RandStrings(length, count)
	case ColumnTypeText, ColumnTypeBlob:
		length := c.arg1
		if length == 0 {
			length = 5
		} else if length > 20 {
			length = 20
		}
		return RandStrings(length, count)
	case ColumnTypeEnum, ColumnTypeSet:
		return RandEnums(c.args, count)
	case ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp:
		return RandDates(count)
	case ColumnTypeTime:
		return RandTimes(count)
	default:
		log.Fatalf("invalid column type %v", c.Tp)
		return nil
	}
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

var numRunes = []rune("0123456789")

func RandNumRunes(n int) string {
	if n == 0 {
		return "0"
	}
	b := make([]rune, n)
	for i := range b {
		b[i] = numRunes[rand.Intn(len(numRunes))]
	}
	return string(b)
}

func RandStrings(strLen int, count int) []string {
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("'%s'", RandStringRunes(rand.Intn(strLen)))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

func RandBigInts(count int) []string {
	nums := make([]int64, count)
	for i := 0; i < count; i++ {
		nums[i] = rand.Int63()
		if RandomBool() {
			nums[i] = -nums[i]
		}
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = strconv.FormatInt(nums[i], 10)
	}
	return result
}

func RandFloats(m, d int, count int) []string {
	nums := make([]float64, count)
	for i := 0; i < count; i++ {
		if m == 0 && d == 0 {
			nums[i] = RandomFloat(0, 10000)
			continue
		}
		left := rand.Intn(1 + mathutil.Min(m-d, 6))
		right := rand.Intn(1 + mathutil.Min(d, 4))
		nums[i], _ = strconv.ParseFloat(fmt.Sprintf("%s.%s", RandNumRunes(left), RandNumRunes(right)), 64)
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("%v", nums[i])
	}
	return result
}

func RandEnums(args []string, count int) []string {
	nums := make([]int, count)
	for i := 0; i < count; i++ {
		nums[i] = rand.Intn(len(args))
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("'%s'", args[nums[i]])
	}
	return result
}

func RandDates(count int) []string {
	return RandGoTimes(count, "2006-01-02")
}

func RandTimes(count int) []string {
	return RandGoTimes(count, "15:04:05.00")
}

func RandGoTimes(count int, format string) []string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2037, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	times := make([]time.Time, count)
	for i := 0; i < count; i++ {
		sec := rand.Int63n(delta) + min
		times[i] = time.Unix(sec, 0)
	}
	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("'%s'", times[i].Format(format))
	}
	return result
}

func RandomFloats(low, high float64, count int) []string {
	nums := make([]float64, count)
	for i := 0; i < count; i++ {
		nums[i] = low + rand.Float64()*(high-low)
	}
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("%f", nums[i])
	}
	return result
}
