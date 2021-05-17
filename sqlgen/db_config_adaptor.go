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
		s.StoreConfig(ConfigKeyUnitFirstColumnIndexable, NewScopeObj(struct{}{}))
		s.StoreConfig(ConfigKeyUnitIndexMergeHint, NewScopeObj(struct{}{}))
		s.StoreConfig(ConfigKeyUnitIndexMergePredicate, NewScopeObj(struct{}{}))
	}
	if w.CreateTable_WithClusterHint {
		s.StoreConfig(ConfigKeyUnitPKNeedClusteredHint, NewScopeObj(struct{}{}))
	}
	if w.CreateTable_MustStrCol {
		s.StoreConfig(ConfigKeyArrayAllowColumnTypes, NewScopeObj([]ColumnType{ColumnTypeChar}))
	} else if w.CreateTable_MustIntCol {
		s.StoreConfig(ConfigKeyArrayAllowColumnTypes, NewScopeObj([]ColumnType{ColumnTypeInt}))
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
		s.StoreConfig(ConfigKeyArrayAllowColumnTypes, NewScopeObj(allowTypes))
	}
	if w.Query_OrderLimit != ConfigKeyEnumLOBOrderBy && w.Query_OrderLimit != ConfigKeyEnumLOBLimitOrderBy {
		if w.Query_HasOrderby > 0 {
			s.StoreConfig(ConfigKeyEnumLimitOrderBy, NewScopeObj(ConfigKeyEnumLOBOrderBy))
		}
		if w.Query_HasLimit > 0 {
			s.StoreConfig(ConfigKeyEnumLimitOrderBy, NewScopeObj(ConfigKeyEnumLOBLimitOrderBy))
		}
	} else {
		s.StoreConfig(ConfigKeyEnumLimitOrderBy, NewScopeObj(w.Query_OrderLimit))
	}
	if !w.Query_DML_Can_Be_Replace {
		s.StoreConfig(ConfigKeyEnumInsertOrReplace, NewScopeObj(ConfigKeyEnumIORInsert))
	}
	if w.CreateTable_MustPrefixIndex {
		s.StoreConfig(ConfigKeyProbabilityIndexPrefix, NewScopeObj(100*Percent))
	}
}
