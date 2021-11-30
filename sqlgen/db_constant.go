package sqlgen

import (
	"fmt"
	"github.com/cznic/mathutil"
)

func (c *Column) EstimateSizeInBytes() int {
	const bytesPerChar = 4
	switch c.Tp {
	case ColumnTypeInt:
		return 4
	case ColumnTypeBoolean, ColumnTypeTinyInt, ColumnTypeYear:
		return 1
	case ColumnTypeSmallInt:
		return 2
	case ColumnTypeMediumInt:
		return 3
	case ColumnTypeBigInt:
		return 8
	case ColumnTypeFloat:
		return 4
	case ColumnTypeDouble, ColumnTypeDecimal:
		return 8
	case ColumnTypeBit:
		return mathutil.Max(c.arg1, 1)
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob:
		return bytesPerChar * c.arg1
	case ColumnTypeBinary, ColumnTypeVarBinary:
		return c.arg1
	case ColumnTypeEnum:
		return 2
	case ColumnTypeSet:
		return 8
	case ColumnTypeDate, ColumnTypeTime:
		return 3
	case ColumnTypeDatetime:
		return 8
	case ColumnTypeTimestamp:
		return 4
	case ColumnTypeJSON:
		return c.arg1
	}
	panic(fmt.Sprintf("unknown column type %d", c.Tp))
	return 0
}

type ColumnType int64

const (
	ColumnTypeInt ColumnType = iota
	ColumnTypeTinyInt
	ColumnTypeSmallInt
	ColumnTypeMediumInt
	ColumnTypeBigInt
	ColumnTypeBoolean
	ColumnTypeFloat
	ColumnTypeDouble
	ColumnTypeDecimal
	ColumnTypeBit

	ColumnTypeChar
	ColumnTypeVarchar
	ColumnTypeText
	ColumnTypeBlob
	ColumnTypeBinary
	ColumnTypeVarBinary
	ColumnTypeEnum
	ColumnTypeSet

	ColumnTypeDate
	ColumnTypeTime
	ColumnTypeDatetime
	ColumnTypeTimestamp
	ColumnTypeYear
	ColumnTypeJSON

	ColumnTypeMax
)

type ColumnTypes []ColumnType

func (tps ColumnTypes) Clone() ColumnTypes {
	ret := make(ColumnTypes, len(tps))
	for i, r := range tps {
		ret[i] = r
	}
	return ret
}

func (tps ColumnTypes) Filter(pred func(tp ColumnType) bool) ColumnTypes {
	ret := make(ColumnTypes, 0, len(tps)/2)
	for _, tp := range tps {
		if pred(tp) {
			ret = append(ret, tp)
		}
	}
	return ret
}

func (tps ColumnTypes) Concat(other ColumnTypes) ColumnTypes {
	ret := make(ColumnTypes, 0, len(tps)+len(other))
	for _, tp := range tps {
		ret = append(ret, tp)
	}
	for _, tp := range other {
		ret = append(ret, tp)
	}
	return ret
}

func (tps ColumnTypes) Contain(targetTp ColumnType) bool {
	for _, tp := range tps {
		if targetTp == tp {
			return true
		}
	}
	return false
}

var ColumnTypeAllTypes = ColumnTypes{
	ColumnTypeInt,
	ColumnTypeTinyInt,
	ColumnTypeSmallInt,
	ColumnTypeMediumInt,
	ColumnTypeBigInt,
	ColumnTypeBoolean,
	ColumnTypeFloat,
	ColumnTypeDouble,
	ColumnTypeDecimal,
	ColumnTypeBit,

	ColumnTypeChar,
	ColumnTypeVarchar,
	ColumnTypeText,
	ColumnTypeBlob,
	ColumnTypeBinary,
	ColumnTypeVarBinary,
	ColumnTypeEnum,
	ColumnTypeSet,

	ColumnTypeDate,
	ColumnTypeTime,
	ColumnTypeDatetime,
	ColumnTypeTimestamp,
	ColumnTypeYear,
	ColumnTypeJSON,
}

var ColumnTypeIntegerTypes = ColumnTypes{
	ColumnTypeInt, ColumnTypeTinyInt, ColumnTypeSmallInt,
	ColumnTypeMediumInt, ColumnTypeBigInt, ColumnTypeBoolean,
}

var ColumnTypeFloatingTypes = ColumnTypes{
	ColumnTypeFloat, ColumnTypeDouble, ColumnTypeDecimal,
}

var ColumnTypeStringTypes = ColumnTypes{
	ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText, ColumnTypeBlob, ColumnTypeBinary,
	ColumnTypeVarBinary, ColumnTypeEnum, ColumnTypeSet,
}

var ColumnTypeTimeTypes = ColumnTypes{
	ColumnTypeDate, ColumnTypeTime, ColumnTypeDatetime, ColumnTypeTimestamp,
}

type Collation struct {
	ID            int
	CharsetName   string
	CollationName string
	IsDefault     bool
}

type CollationType int64

const (
	CollationBinary CollationType = iota
	CollationUtf8Bin
	CollationUtf8mb4Bin
	CollationUtf8GeneralCI
	CollationUtf8mb4GeneralCI
	CollationUtf8UnicodeCI
	CollationUtf8mb4UnicodeCI
	CollationGBKBin
	CollationGBKChineseCI

	CollationTypeMax
)

var Collations = map[CollationType]*Collation{
	CollationGBKChineseCI:     {28, "gbk", "gbk_chinese_ci", true},
	CollationUtf8GeneralCI:    {33, "utf8", "utf8_general_ci", false},
	CollationUtf8mb4GeneralCI: {45, "utf8mb4", "utf8mb4_general_ci", false},
	CollationUtf8mb4Bin:       {46, "utf8mb4", "utf8mb4_bin", true},
	CollationBinary:           {63, "binary", "binary", true},
	CollationUtf8Bin:          {83, "utf8", "utf8_bin", true},
	CollationGBKBin:           {87, "gbk", "gbk_bin", false},
	CollationUtf8UnicodeCI:    {192, "utf8", "utf8_unicode_ci", false},
	CollationUtf8mb4UnicodeCI: {224, "utf8mb4", "utf8mb4_unicode_ci", false},
}

func (c ColumnType) IsStringType() bool {
	switch c {
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText,
		ColumnTypeBlob, ColumnTypeBinary, ColumnTypeVarBinary:
		return true
	}
	return false
}

func (c ColumnType) SameTypeAs(other ColumnType) bool {
	return (c.IsStringType() && other.IsStringType()) || (c.IsIntegerType() && other.IsIntegerType())
}

func (c ColumnType) RequiredFieldLength() bool {
	return c == ColumnTypeVarchar || c == ColumnTypeVarBinary
}

func (c ColumnType) NeedKeyLength() bool {
	return c == ColumnTypeBlob || c == ColumnTypeText
}

func (c ColumnType) IsIntegerType() bool {
	switch c {
	case ColumnTypeInt, ColumnTypeTinyInt, ColumnTypeSmallInt, ColumnTypeMediumInt, ColumnTypeBigInt:
		return true
	}
	return false
}

func (c ColumnType) IsPartitionType() bool {
	// A partitioning key must be either an integer column or
	// an expression that resolves to an integer.
	return c.IsIntegerType()
}

func (c ColumnType) IsPointGetableType() bool {
	switch c {
	case ColumnTypeFloat, ColumnTypeDouble, ColumnTypeText, ColumnTypeBlob:
		return false
	}
	return true
}

// BLOB/TEXT/JSON column can't have a default value.
func (c ColumnType) DisallowDefaultValue() bool {
	// ERROR 1101 (42000): BLOB/TEXT/JSON column 'a' can't have a default value
	return c == ColumnTypeText || c == ColumnTypeBlob || c == ColumnTypeJSON
}

func (c ColumnType) String() string {
	switch c {
	case ColumnTypeInt:
		return "int"
	case ColumnTypeBoolean:
		return "boolean"
	case ColumnTypeTinyInt:
		return "tinyint"
	case ColumnTypeSmallInt:
		return "smallint"
	case ColumnTypeMediumInt:
		return "mediumint"
	case ColumnTypeBigInt:
		return "bigint"
	case ColumnTypeFloat:
		return "float"
	case ColumnTypeDouble:
		return "double"
	case ColumnTypeDecimal:
		return "decimal"
	case ColumnTypeBit:
		return "bit"
	case ColumnTypeChar:
		return "char"
	case ColumnTypeVarchar:
		return "varchar"
	case ColumnTypeText:
		return "text"
	case ColumnTypeBlob:
		return "blob"
	case ColumnTypeBinary:
		return "binary"
	case ColumnTypeVarBinary:
		return "varbinary"
	case ColumnTypeEnum:
		return "enum"
	case ColumnTypeSet:
		return "set"
	case ColumnTypeDate:
		return "date"
	case ColumnTypeTime:
		return "time"
	case ColumnTypeDatetime:
		return "datetime"
	case ColumnTypeTimestamp:
		return "timestamp"
	case ColumnTypeYear:
		return "year"
	case ColumnTypeJSON:
		return "json"
	default:
		return fmt.Sprintf("unknown: %d", c)
	}
}

type IndexType int64

const (
	IndexTypeNonUnique IndexType = iota
	IndexTypeUnique
	IndexTypePrimary
)

type ScopeKeyType int8

const (
	ScopeKeyCurrentFn ScopeKeyType = iota
	ScopeKeyCurrentTables
	ScopeKeyCurrentSelectedColNum
	ScopeKeyCurrentPrepare
	ScopeKeyCurrentPartitionColumn
	ScopeKeyCurrentModifyColumn
	ScopeKeyCurrentUniqueIndexForPointGet
	ScopeKeyCurrentSelectedColumns
	ScopeKeyCurrentOrderByColumns
	ScopeKeyLastOutFileTable
	ScopeKeyJoinPreferIndex

	ScopeKeyTableUniqID
	ScopeKeyColumnUniqID
	ScopeKeyIndexUniqID
	ScopeKeyTmpFileID
	ScopeKeyPrepareID
	ScopeKeyCTEUniqID
	ScopeKeyCTEAsNameID
)

const DefaultKeySizeLimit = 3072

const SelectOutFileDir = "/tmp/tidb_tp_test_outfile"

type StateClearOption int8

const (
	StateClearOptionWeight StateClearOption = iota
	StateClearOptionRepeat
	StateClearOptionConfig
	StateClearOptionScope
	StateClearOptionAll
)
