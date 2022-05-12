package sqlgen

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cznic/mathutil"
)

var ColumnDefinitions = NewFn(func(state *State) Fn {
	return Repeat(ColumnDefinition.R(1, 10), Str(","))
})

var ColumnDefinition = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	partialCol := &Column{ID: state.alloc.AllocColumnID()}
	state.env.Column = partialCol
	// Example:
	//   a varchar(255) collate utf8mb4_bin not null
	//   b bigint unsigned default 100
	ret, err := And(
		ColumnDefinitionName,
		ColumnDefinitionTypeOnCreate,
		ColumnDefinitionCollation,
		ColumnDefinitionUnsigned,
		ColumnDefinitionNotNull,
		ColumnDefinitionDefault,
	).Eval(state)
	if err != nil {
		return NoneBecauseOf(err)
	}
	tbl.AppendColumn(partialCol)
	return Str(ret)
})

var ColumnDefinitionName = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Name = fmt.Sprintf("col_%d", col.ID)
	return Str(col.Name)
})

var ColumnDefinitionTypeOnCreate = NewFn(func(state *State) Fn {
	return ColumnDefinitionType
})

var ColumnDefinitionTypeOnAdd = NewFn(func(state *State) Fn {
	return ColumnDefinitionType
})

var ColumnDefinitionTypeOnModify = NewFn(func(state *State) Fn {
	return ColumnDefinitionType
})

var ColumnDefinitionType = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesIntegers.W(5),
		ColumnDefinitionTypesFloatings.W(3),
		ColumnDefinitionTypesStrings.W(6),
		ColumnDefinitionTypesBinaries.W(3),
		ColumnDefinitionTypesTimes.W(5),
		ColumnDefinitionTypesBit,
		ColumnDefinitionTypesJSON,
	)
})

var ColumnDefinitionCollation = NewFn(func(state *State) Fn {
	col := state.env.Column
	if !col.Tp.IsStringType() {
		return Empty
	}
	switch col.Tp {
	case ColumnTypeBinary, ColumnTypeVarBinary, ColumnTypeBlob:
		col.Collation = Collations[CollationBinary]
		return Empty
	default:
		col.Collation = Collations[CollationType(rand.Intn(int(CollationTypeMax)-1)+1)]
		if oldCol := state.env.OldColumn; oldCol != nil && !oldCol.Collation.CharsetCompatible(col.Collation) {
			return Empty
		}
		return Opt(Strs("collate", col.Collation.CollationName))
	}
})

var ColumnDefinitionNotNull = NewFn(func(state *State) Fn {
	col := state.env.Column
	if RandomBool() {
		col.isNotNull = true
		return Str("not null")
	} else {
		return Empty
	}
})

var ColumnDefinitionDefault = NewFn(func(state *State) Fn {
	col := state.env.Column
	if RandomBool() || col.Tp.DisallowDefaultValue() {
		return Empty
	}
	return Strs("default", col.RandomValue())
})

var ColumnDefinitionUnsigned = NewFn(func(state *State) Fn {
	col := state.env.Column
	if !col.Tp.IsIntegerType() {
		return Empty
	}
	if RandomBool() {
		col.isUnsigned = true
		return Str("unsigned")
	} else {
		return Empty
	}
})

var ColumnDefinitionTypesStrings = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesChar,
		ColumnDefinitionTypesVarchar,
		ColumnDefinitionTypesText,

		ColumnDefinitionTypesEnum,
		ColumnDefinitionTypesSet,
	)
})

var ColumnDefinitionTypesBinaries = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesBlob,
		ColumnDefinitionTypesBinary,
		ColumnDefinitionTypesVarbinary,
	)
})

var ColumnDefinitionTypesTimes = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesDate,
		ColumnDefinitionTypesTime,
		ColumnDefinitionTypesDateTime,
		ColumnDefinitionTypesTimestamp,
		ColumnDefinitionTypesYear,
	)
})

var ColumnDefinitionTypesIntegers = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesIntegerBool,
		ColumnDefinitionTypesIntegerTiny,
		ColumnDefinitionTypesIntegerSmall,
		ColumnDefinitionTypesIntegerInt,
		ColumnDefinitionTypesIntegerMedium,
		ColumnDefinitionTypesIntegerBig,
	)
})

var ColumnDefinitionTypesFloatings = NewFn(func(state *State) Fn {
	return Or(
		ColumnDefinitionTypesFloat,
		ColumnDefinitionTypesDouble,
		ColumnDefinitionTypesDecimal,
	)
})

var ColumnDefinitionTypesIntegerBool = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeBoolean
	return Str("boolean")
})

var ColumnDefinitionTypesIntegerTiny = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeTinyInt
	return Str("tinyint")
})

var ColumnDefinitionTypesIntegerSmall = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeSmallInt
	return Str("smallint")
})

var ColumnDefinitionTypesIntegerMedium = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeMediumInt
	return Str("mediumint")
})

var ColumnDefinitionTypesIntegerInt = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeInt
	return Str("int")
})

var ColumnDefinitionTypesIntegerBig = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeBigInt
	return Str("bigint")
})

var ColumnDefinitionTypesFloat = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 0
	col.arg2 = 0
	col.Tp = ColumnTypeFloat
	return Str("float")
})

var ColumnDefinitionTypesDouble = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 0
	col.arg2 = 0
	col.Tp = ColumnTypeDouble
	return Str("double")
})

var ColumnDefinitionTypesDecimal = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(65)
	upper := mathutil.Min(col.arg1, 30)
	col.arg2 = 1 + rand.Intn(upper)
	col.Tp = ColumnTypeDecimal
	return Strs("decimal", "(", Num(col.arg1), ",", Num(col.arg2), ")")
})

var ColumnDefinitionTypesBit = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(62)
	col.Tp = ColumnTypeBit
	return Strs("bit", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesChar = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(255)
	col.Tp = ColumnTypeChar
	return Strs("char", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesBinary = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(255)
	col.Tp = ColumnTypeBinary
	return Strs("binary", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesVarchar = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeVarchar
	return Strs("varchar", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesText = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeText
	return Strs("text", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesBlob = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeBlob
	return Strs("blob", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesVarbinary = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.arg1 = 1 + rand.Intn(512)
	col.Tp = ColumnTypeVarBinary
	return Strs("varbinary", "(", Num(col.arg1), ")")
})

var ColumnDefinitionTypesEnum = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.args = []string{"Alice", "Bob", "Charlie", "David"}
	col.Tp = ColumnTypeEnum
	var sb strings.Builder
	for i, v := range col.args {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("'")
		sb.WriteString(v)
		sb.WriteString("'")
	}
	return Strs("enum", "(", sb.String(), ")")
})

var ColumnDefinitionTypesSet = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.args = []string{"Alice", "Bob", "Charlie", "David"}
	col.Tp = ColumnTypeSet
	var sb strings.Builder
	for i, v := range col.args {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString("'")
		sb.WriteString(v)
		sb.WriteString("'")
	}
	return Strs("set", "(", sb.String(), ")")
})

var ColumnDefinitionTypesDate = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeDate
	return Str("date")
})

var ColumnDefinitionTypesTime = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeTime
	return Str("time")
})

var ColumnDefinitionTypesDateTime = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeDatetime
	return Str("datetime")
})

var ColumnDefinitionTypesTimestamp = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeTimestamp
	return Str("timestamp")
})

var ColumnDefinitionTypesYear = NewFn(func(state *State) Fn {
	col := state.env.Column
	col.Tp = ColumnTypeYear
	return Str("year")
})

var ColumnDefinitionTypesJSON = NewFn(func(state *State) Fn {
	tbl := state.env.Table
	if state.env.OldColumn != nil && tbl.Indexes.Found(func(index *Index) bool {
		return index.HasColumn(state.env.OldColumn)
	}) {
		// JSON column cannot be used in key specification.
		return None("json column cannot be used in key")
	}
	col := state.env.Column
	col.Tp = ColumnTypeJSON
	return Str("json")
})
