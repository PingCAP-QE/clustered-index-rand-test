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

func (s *State) GenNewTable() *Table {
	id := s.alloc.AllocTableID()
	tblName := fmt.Sprintf("tbl_%d", id)
	newTbl := &Table{ID: id, Name: tblName}
	// newTbl.Collate = Collations[CollationType(rand.Intn(int(CollationTypeMax)-1)+1)]
	newTbl.Collate = Collations[CollationBinary]
	newTbl.childTables = []*Table{newTbl}
	return newTbl
}

func (s *State) GenNewCTE() *Table {
	id := s.alloc.AllocCTEID()
	return &Table{
		ID:   -1, // we do not use the id in CTE
		Name: fmt.Sprintf("cte_%d", id),
	}
}

func (s *State) GenNewColumnWithType(tps ...ColumnType) *Column {
	id := s.alloc.AllocColumnID()
	col := &Column{ID: id, Name: fmt.Sprintf("col_%d", id)}
	col.Tp = tps[rand.Intn(len(tps))]
	switch col.Tp {
	// https://docs.pingcap.com/tidb/stable/data-type-numeric
	case ColumnTypeFloat, ColumnTypeDouble:
		// Float/Double precision is deprecated.
		// https://github.com/pingcap/tidb/issues/21692
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
	if !col.Tp.RequiredFieldLength() && rand.Intn(5) == 0 {
		col.arg1, col.arg2 = 0, 0
	}
	// Set collation
	if col.Tp == ColumnTypeBinary || col.Tp == ColumnTypeBlob || col.Tp == ColumnTypeVarBinary {
		col.Collation = Collations[CollationBinary]
	} else {
		col.Collation = Collations[CollationType(rand.Intn(int(CollationTypeMax)-1)+1)]
	}

	if col.Tp.IsIntegerType() {
		col.isUnsigned = RandomBool()
	}
	col.isNotNull = RandomBool()
	if !col.Tp.DisallowDefaultValue() && RandomBool() {
		col.defaultVal = col.RandomValue()
	}
	return col
}

func GenPrefixLen(state *State, cols []*Column) []int {
	prefixLens := make([]int, len(cols))
	for i, c := range cols {
		if c.Tp.NeedKeyLength() || (c.Tp.IsStringType() && c.arg1 > 0 && RandomBool()) {
			maxLength := mathutil.Min(c.arg1, 5)
			if maxLength == 0 {
				maxLength = 5
			}
			prefixLens[i] = 1 + rand.Intn(maxLength)
		}
	}
	return prefixLens
}

func LimitIndexColumnSize(cols []*Column, sizeLimit int) []*Column {
	maxIdx, keySize := len(cols), 0
	for i, c := range cols {
		keySize += c.EstimateSizeInBytes()
		if keySize > sizeLimit {
			maxIdx = i
			break
		}
	}
	Assert(maxIdx > 0)
	return cols[:maxIdx]
}

func GenNewPrepare(id int) *Prepare {
	return &Prepare{
		ID:   id,
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
	pkIdx := t.Indexes.Primary()
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
	case ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeInt, ColumnTypeBigInt, ColumnTypeBoolean, ColumnTypeYear:
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
	case ColumnTypeJSON:
		return "NULL"
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
		return RandStrings(length, count, c.Collation.CharsetName == "gbk")
	case ColumnTypeText, ColumnTypeBlob:
		length := c.arg1
		if length == 0 {
			length = 5
		} else if length > 20 {
			length = 20
		}
		return RandStrings(length, count, c.Collation.CharsetName == "gbk")
	case ColumnTypeEnum, ColumnTypeSet:
		return RandEnums(c.args, count)
	case ColumnTypeDate, ColumnTypeDatetime, ColumnTypeTimestamp:
		return RandDates(count)
	case ColumnTypeTime:
		return RandTimes(count)
	case ColumnTypeYear:
		return RandYear(count)
	case ColumnTypeJSON:
		return RandJsons(count)
	default:
		log.Fatalf("invalid column type %v", c.Tp)
		return nil
	}
}

type JSONType uint8

const (
	JSONTypeCodeObject JSONType = iota
	JSONTypeCodeArray
	JSONTypeCodeLiteral
	JSONTypeCodeInt64
	JSONTypeCodeUint64
	JSONTypeCodeFloat64
	JSONTypeCodeString
)

var jMap = map[JSONType][]string{}

func init() {
	jMap[JSONTypeCodeObject] = []string{`"{\"obj0\": 10}"`, `"{\"obj1\": {\"sub_obj0\":100}}"`}
	jMap[JSONTypeCodeArray] = []string{`"[-1, 0, 1]"`, `"[3, 2, 1]"`, `"[1, 1, 1]"`}
	jMap[JSONTypeCodeLiteral] = []string{`"null"`, `"true"`, `"false"`}
	jMap[JSONTypeCodeInt64] = []string{`"-22"`, `"33"`, `"-44"`}
	jMap[JSONTypeCodeUint64] = []string{`"55"`, `"66"`, `"77"`}
	jMap[JSONTypeCodeFloat64] = []string{`"323232323.3232323232"`, `"12121212.1212121212"`}
	jMap[JSONTypeCodeString] = []string{`"\"json string1\""`, `"\"json string2\""`, `"\"json string3\""`}
}

func RandJsons(count int) []string {
	res := make([]string, 0, count)
	for i := 0; i < count; i++ {
		jsonType := JSONType(rand.Intn(len(jMap)))
		length := len(jMap[jsonType])
		res = append(res, jMap[jsonType][rand.Intn(length)])
	}
	return res
}

var asciiRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789~!@#$%^&*()_+=-")

func RandStringRunes(n int, mixCNChar bool) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = asciiRunes[rand.Intn(len(asciiRunes))]
		if mixCNChar && rand.Intn(3) == 0 {
			b[i] = rune(int('\u4e00') + rand.Intn(int('\u9fff')-int('\u4e00')))
		}
	}
	return string(b)
}

func RandGBKStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = rune(int('\u4e00') + rand.Intn(int('\u9fff')-int('\u4e00')))
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

func RandStrings(strLen int, count int, mixCNChar bool) []string {
	result := make([]string, count)
	for i := 0; i < count; i++ {
		result[i] = fmt.Sprintf("'%s'", RandStringRunes(rand.Intn(strLen), mixCNChar))
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

func RandYear(count int) []string {
	return RandGoTimes(count, "2006")
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
