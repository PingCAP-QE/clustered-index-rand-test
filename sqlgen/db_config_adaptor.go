package sqlgen

// setupReplacer is set for backward compatibility.
// New users should define their own replacer for different scenarios.
func setupReplacer(s *State) {
	repl := NewFnHookReplacer()
	setWeight(s)
	setRepeat(s)
	mapConfigKey(s)
	s.AppendHook(repl)
}

func setWeight(s *State) {
	w := s.ctrl.Weight
	s.SetWeight(OnDuplicateUpdate, w.Query_DML_INSERT_ON_DUP)
	if tp := w.CreateTable_Partition_Type; tp != "" {
		Assert(tp == "hash" || tp == "range" || tp == "list")
		const hash, rangE, list = 0, 1, 2
		weight := []int{0, 0, 0}
		switch tp {
		case "hash":
			weight[hash] = 1
		case "range":
			weight[rangE] = 1
		case "list":
			weight[list] = 1
		}
		s.SetWeight(PartitionDefinitionHash, weight[hash])
		s.SetWeight(PartitionDefinitionRange, weight[rangE])
		s.SetWeight(PartitionDefinitionList, weight[list])
	}
	if w.CreateTable_WithoutLike > 0 {
		s.SetWeight(CreateTableLike, 0)
	}
}

func setRepeat(s *State) {
	w := s.ctrl.Weight
	if w.CreateTable_MoreCol > 0 {
		s.SetRepeat(ColumnDefinition, 1, w.CreateTable_MoreCol)
	}
	s.SetRepeat(IndexDefinition, 1, w.CreateTable_IndexMoreCol)
}

func mapConfigKey(s *State) {
	w := s.ctrl.Weight
	if w.Query_INDEX_MERGE {
		s.StoreConfig(ConfigKeyUnitFirstColumnIndexable, struct{}{})
		s.StoreConfig(ConfigKeyUnitIndexMergeHint, struct{}{})
		s.StoreConfig(ConfigKeyUnitIndexMergePredicate, struct{}{})
	}
	if w.CreateTable_WithClusterHint {
		s.StoreConfig(ConfigKeyUnitPKNeedClusteredHint, struct{}{})
	}
	if w.CreateTable_MustStrCol {
		s.StoreConfig(ConfigKeyArrayAllowColumnTypes, []ColumnType{ColumnTypeChar})
	} else if w.CreateTable_MustIntCol {
		s.StoreConfig(ConfigKeyArrayAllowColumnTypes, []ColumnType{ColumnTypeInt})
	} else if len(w.CreateTable_IgnoredTypeCols) > 0 {
		allowTypes := make([]ColumnType, 0, len(ColumnTypeAllTypes))
		for _, c := range ColumnTypeAllTypes {
			needIgnore := false
			for _, ignoreCol := range w.CreateTable_IgnoredTypeCols {
				if c == ignoreCol {
					needIgnore = true
					break
				}
			}
			if !needIgnore {
				allowTypes = append(allowTypes, c)
			}
		}
		s.StoreConfig(ConfigKeyArrayAllowColumnTypes, allowTypes)
	}
	if w.Query_OrderLimit != ConfigKeyEnumLOBOrderBy && w.Query_OrderLimit != ConfigKeyEnumLOBLimitOrderBy {
		if w.Query_HasOrderby > 0 {
			s.StoreConfig(ConfigKeyEnumLimitOrderBy, ConfigKeyEnumLOBOrderBy)
		}
		if w.Query_HasLimit > 0 {
			s.StoreConfig(ConfigKeyEnumLimitOrderBy, ConfigKeyEnumLOBLimitOrderBy)
		}
	} else {
		s.StoreConfig(ConfigKeyEnumLimitOrderBy, w.Query_OrderLimit)
	}
	if !w.Query_DML_Can_Be_Replace {
		s.StoreConfig(ConfigKeyEnumInsertOrReplace, ConfigKeyEnumIORInsert)
	}
	if w.CreateTable_MustPrefixIndex {
		s.StoreConfig(ConfigKeyProbabilityIndexPrefix, 100*Percent)
	}
	if s.ctrl.MaxTableNum > 0 {
		s.StoreConfig(ConfigKeyIntMaxTableCount, s.ctrl.MaxTableNum)
	}
	if s.ctrl.StrictTransTable {
		s.StoreConfig(ConfigKeyUnitStrictTransTable, struct{}{})
	}
}
