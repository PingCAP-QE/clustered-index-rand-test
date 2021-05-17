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
	case ColumnTypeBoolean, ColumnTypeTinyInt:
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

	ColumnTypeMax
)

type CollationType int64

const (
	CollationBinary CollationType = iota
	CollationUtf8Bin
	CollationUtf8mb4Bin
	CollationUtf8GeneralCI
	CollationUtf8mb4GeneralCI
	CollationUtf8UnicodeCI
	CollationUtf8mb4UnicodeCI

	CollationTypeMax
)

func (c CollationType) String() string {
	switch c {
	case CollationBinary:
		return "binary"
	case CollationUtf8Bin:
		return "utf8_bin"
	case CollationUtf8mb4Bin:
		return "utf8mb4_bin"
	case CollationUtf8GeneralCI:
		return "utf8_general_ci"
	case CollationUtf8mb4GeneralCI:
		return "utf8mb4_general_ci"
	case CollationUtf8UnicodeCI:
		return "utf8_unicode_ci"
	case CollationUtf8mb4UnicodeCI:
		return "utf8mb4_unicode_ci"
	}
	return "invalid type"
}

func (c ColumnType) IsStringType() bool {
	switch c {
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText,
		ColumnTypeBlob, ColumnTypeBinary, ColumnTypeVarBinary:
		return true
	}
	return false
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
	return c == ColumnTypeText || c == ColumnTypeBlob
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
	}
	return "invalid type"
}

type IndexType int64

const (
	IndexTypeNonUnique IndexType = iota
	IndexTypeUnique
	IndexTypePrimary

	IndexTypeMax
)

type ScopeKeyType int8

const (
	ScopeKeyCurrentFn ScopeKeyType = iota
	ScopeKeyCurrentTables
	ScopeKeyCurrentSelectedColNum
	ScopeKeyCurrentPrepare
	ScopeKeyCurrentPartitionColumn
	ScopeKeyCurrentUniqueIndexForPointGet
	ScopeKeyCurrentSelectedColumns
	ScopeKeyCurrentOrderByColumns
	ScopeKeyLastOutFileTable

	ScopeKeyTableUniqID
	ScopeKeyColumnUniqID
	ScopeKeyIndexUniqID
	ScopeKeyTmpFileID
	ScopeKeyPrepareID
)

type ConfigKeyType int64

const (
	ConfigKeyNone ConfigKeyType = iota
	ConfigKeyProbabilityIndexPrefix
	ConfigKeyUnitFirstColumnIndexable
	ConfigKeyUnitPKNeedClusteredHint
	ConfigKeyUnitIndexMergeHint
	ConfigKeyEnumLimitOrderBy    // value should be "none", "order-by", "limit-order-by"
	ConfigKeyEnumColumnType      // value should be "int" or "string".
	ConfigKeyEnumInsertOrReplace // value should be "insert" or "replace"
)

const (
	ConfigKeyEnumLOBNone         = "none"
	ConfigKeyEnumLOBOrderBy      = "order-by"
	ConfigKeyEnumLOBLimitOrderBy = "limit-order-by"
	ConfigKeyEnumIORInsert       = "insert"
	ConfigKeyEnumIORReplace      = "replace"
)

const DefaultKeySize = 3072

const Percent = 1
const ProbabilityMax = 100 * Percent

const SelectOutFileDir = "/tmp/tidb_tp_test_outfile"
