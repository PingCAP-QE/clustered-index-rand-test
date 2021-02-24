package sqlgen

import (
	"fmt"

	"github.com/cznic/mathutil"
)

func (c *Column) EstimateSizeInBytes() int {
	const bytesPerChar = 4
	switch c.tp {
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
	case ColumnTypeBinary:
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
	panic(fmt.Sprintf("unknown column type %d", c.tp))
	return 0
}

type ColumnType int64

const (
	ColumnTypeInt ColumnType = iota
	ColumnTypeBoolean
	ColumnTypeTinyInt
	ColumnTypeSmallInt
	ColumnTypeMediumInt
	ColumnTypeBigInt
	ColumnTypeFloat
	ColumnTypeDouble
	ColumnTypeDecimal
	ColumnTypeBit

	ColumnTypeChar
	ColumnTypeVarchar
	ColumnTypeText
	ColumnTypeBlob
	ColumnTypeBinary
	ColumnTypeEnum
	ColumnTypeSet

	ColumnTypeDate
	ColumnTypeTime
	ColumnTypeDatetime
	ColumnTypeTimestamp

	ColumnTypeMax
)

func (c ColumnType) IsStringType() bool {
	switch c {
	case ColumnTypeChar, ColumnTypeVarchar, ColumnTypeText,
		ColumnTypeBlob, ColumnTypeBinary:
		return true
	}
	return false
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
	ScopeKeyCurrentTable ScopeKeyType = iota
	ScopeKeyCurrentColumn
	ScopeKeyCurrentIndex
	ScopeKeyCurrentPrepare
	ScopeKeyLastDropTable
	ScopeKeyCurrentPartitionColumn
	ScopeKeyLastOutFileTable

	ScopeKeySelectedCols

	ScopeKeyTableUniqID
	ScopeKeyColumnUniqID
	ScopeKeyIndexUniqID
	ScopeKeyTmpFileID
	ScopeKeyPrepareID
)

const DefaultKeySize = 3072

const SelectOutFileDir = "/tmp/tidb_tp_test_outfile"
