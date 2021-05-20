package sqlgen

type ConfigKeyType int64

const (
	ConfigKeyNone                     ConfigKeyType = iota
	ConfigKeyProbabilityIndexPrefix                 // value example: 50*Percent
	ConfigKeyProbabilityTiFlashTable                // value example: 50*Percent
	ConfigKeyUnitFirstColumnIndexable               // value example: struct{}{}
	ConfigKeyUnitPKNeedClusteredHint                // value example: struct{}{}
	ConfigKeyUnitIndexMergeHint                     // value example: struct{}{}
	ConfigKeyUnitIndexMergePredicate                // value example: struct{}{}
	ConfigKeyUnitStrictTransTable                   // value example: struct{}{}
	ConfigKeyUnitTiFlashQueryHint                   // value example: struct{}{}
	ConfigKeyEnumLimitOrderBy                       // value should be "none", "order-by", "limit-order-by"
	ConfigKeyArrayAllowColumnTypes                  // value example: []ColumnType{ColumnTypeInt, ColumnTypeTinyInt}
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
