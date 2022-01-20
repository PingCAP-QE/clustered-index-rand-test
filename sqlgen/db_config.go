package sqlgen

import "math/rand"

type ConfigurableState State

func (s *ConfigurableState) SetMaxTable(count int) {
	NoTooMuchTables = func(s *State) bool {
		return len(s.tables) < count
	}
}

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
	ConfigKeyUnitAvoidDropPrimaryKey                // value example: struct{}{}
	ConfigKeyStableOrderBy                          // value example: struct{}{}
	ConfigKeyEnumLimitOrderBy                       // value should be "none", "order-by", "limit-order-by"
	ConfigKeyArrayAllowColumnTypes                  // value example: ColumnTypes{ColumnTypeInt, ColumnTypeTinyInt}
	ConfigKeyIntMaxTableCount                       // value example: 10
	ConfigKeyCTEValidSQLPercent                     // value example: [0,100]
)

const Percent = 1
const ProbabilityMax = 100 * Percent

func ConfigKeyUnitFirstColumnIndexableGenColumns(totalCols Columns) Columns {
	// Make sure the first column is not bit || enum || set.
	firstColCandidates := totalCols.FilterColumnsIndices(func(c *Column) bool {
		return c.Tp != ColumnTypeBit && c.Tp != ColumnTypeEnum && c.Tp != ColumnTypeSet
	})
	if len(firstColCandidates) == 0 {
		totalCols = totalCols.GetRandNonEmpty()
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
