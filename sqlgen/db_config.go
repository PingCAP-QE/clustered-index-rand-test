package sqlgen

type ConfigKeyType int64

const (
	ConfigKeyNone                     ConfigKeyType = iota
	ConfigKeyProbabilityIndexPrefix                 // value example: 50*Percent
	ConfigKeyProbabilityTiFlashTable                // value example: 50*Percent
	ConfigKeyUnitTiFlashQueryHint                   // value example: struct{}{}
	ConfigKeyUnitFirstColumnIndexable               // value example: struct{}{}
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

func ResolveColumnTypes(s *State, extractor func(*ConfigAllowedColumnTypes) ColumnTypes) ColumnTypes {
	if !s.ExistsConfig(ConfigKeyArrayAllowColumnTypes) {
		return ColumnTypeAllTypes
	}
	tpsCfg := s.SearchConfig(ConfigKeyArrayAllowColumnTypes)
	switch v := tpsCfg.obj.(type) {
	case ColumnTypes:
		return v
	case []ColumnType:
		return v
	case *ConfigAllowedColumnTypes:
		tps := extractor(v)
		if len(tps) == 0 {
			tps = v.Default
		}
		if len(tps) == 0 {
			tps = ColumnTypeAllTypes
		}
		return tps
	}
	NeverReach()
	return nil
}
