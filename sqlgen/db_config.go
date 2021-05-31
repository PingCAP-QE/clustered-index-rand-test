package sqlgen

import "math/rand"

type ConfigKeyType int64

const (
	ConfigKeyNone                     ConfigKeyType = iota
	ConfigKeyProbabilityIndexPrefix                 // value example: 50*Percent
	ConfigKeyProbabilityTiFlashTable                // value example: 50*Percent
	ConfigKeyUnitTiFlashQueryHint                   // value example: struct{}{}
	ConfigKeyUnitFirstColumnIndexable               // value example: struct{}{}
	ConfigKeyUnitLimitIndexKeyLength                // value example: struct{}{}
	ConfigKeyUnitPKNeedClusteredHint                // value example: struct{}{}
	ConfigKeyUnitIndexMergeHint                     // value example: struct{}{}
	ConfigKeyUnitIndexMergePredicate                // value example: struct{}{}
	ConfigKeyUnitNonStrictTransTable                // value example: struct{}{}
	ConfigKeyUnitAvoidAlterPKColumn                 // value example: struct{}{}
	ConfigKeyEnumLimitOrderBy                       // value should be "none", "order-by", "limit-order-by"
	ConfigKeyArrayAllowColumnTypes                  // value example: ColumnTypes{ColumnTypeInt, ColumnTypeTinyInt}
	ConfigKeyIntMaxTableCount                       // value example: 10
	ConfigKeyCTEValidSQLPercent                     // value example: [0,100]
)

const (
	ConfigKeyEnumLOBNone         = "none"
	ConfigKeyEnumLOBOrderBy      = "order-by"
	ConfigKeyEnumLOBLimitOrderBy = "limit-order-by"
)

const Percent = 1
const ProbabilityMax = 100 * Percent

type ConfigAllowedColumnTypes struct {
	Default      ColumnTypes
	CreateTable  ColumnTypes
	AddColumn    ColumnTypes
	ModifyColumn ColumnTypes
}

func NewConfigAllowColumnTypes() *ConfigAllowedColumnTypes {
	return &ConfigAllowedColumnTypes{Default: ColumnTypeAllTypes.Clone()}
}

func (c *ConfigAllowedColumnTypes) CreateTableOrDefault() ColumnTypes {
	if len(c.CreateTable) == 0 {
		return c.Default
	}
	return c.CreateTable
}

func (c *ConfigAllowedColumnTypes) AddColumnOrDefault() ColumnTypes {
	if len(c.AddColumn) == 0 {
		return c.Default
	}
	return c.AddColumn
}

func (c *ConfigAllowedColumnTypes) ModifyColumnOrDefault() ColumnTypes {
	if len(c.ModifyColumn) == 0 {
		return c.Default
	}
	return c.ModifyColumn
}

func (s ScopeObj) ToConfigAllowedColumnTypes() *ConfigAllowedColumnTypes {
	cfg := NewConfigAllowColumnTypes()
	if s.IsNil() {
		return cfg
	}
	switch v := s.obj.(type) {
	case ColumnTypes:
		cfg.Default = v
	case []ColumnType:
		cfg.Default = v
	case *ConfigAllowedColumnTypes:
		cfg = v
	default:
		NeverReach()
	}
	return cfg
}

func ConfigKeyUnitFirstColumnIndexableGenColumns(totalCols Columns) Columns {
	// Make sure the first column is not bit || enum || set.
	firstColCandidates := totalCols.FilterColumnsIndices(func(c *Column) bool {
		return c.Tp != ColumnTypeBit && c.Tp != ColumnTypeEnum && c.Tp != ColumnTypeSet
	})
	if len(firstColCandidates) == 0 {
		totalCols = totalCols.GetRandColumnsNonEmpty()
	} else {
		firstColIdx := firstColCandidates[rand.Intn(len(firstColCandidates))]
		totalCols[0], totalCols[firstColIdx] = totalCols[firstColIdx], totalCols[0]
		rand.Shuffle(len(totalCols)-1, func(i, j int) {
			totalCols[i+1], totalCols[j+1] = totalCols[j+1], totalCols[i+1]
		})
		restNum := 0
		if len(totalCols) > 1 {
			restNum = rand.Intn(len(totalCols) - 1)
		}
		totalCols = totalCols[:1+restNum]
	}
	return totalCols
}
