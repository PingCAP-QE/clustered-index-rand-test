package sqlgen

// setupReplacer is set for backward compatibility.
// New users should define their own replacer for different scenarios.
func setupReplacer(s *State) {
	w := s.ctrl.Weight
	repl := NewFnHookReplacer()
	setPartitionType(repl, w.CreateTable_Partition_Type)
	setIndexColumnCountHint(repl, w.CreateTable_IndexMoreCol, w.Query_INDEX_MERGE)
	setColumnCountHint(repl, w.CreateTable_MaxColumnCnt)
	mapConfigKey(s)
	s.AppendHook(repl)
}

func setPartitionType(repl *FnHookReplacer, tp string) {
	if tp == "" {
		return
	}
	Assert(tp == "hash" || tp == "range" || tp == "list")
	repl.Replace(PartitionDefinition, NewFn(func(state *State) Fn {
		tbl := state.Search(ScopeKeyCurrentTables).ToTables().One()
		partCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
		tbl.AppendPartitionColumn(partCol)
		switch tp {
		case "hash":
			return PartitionDefinitionHash
		case "range":
			return PartitionDefinitionRange
		case "list":
			return PartitionDefinitionList
		}
		return Empty()
	}))
}

func setIndexColumnCountHint(repl *FnHookReplacer, more int, must bool) {
	Assert(more >= 1)
	repl.Replace(IndexDefinitions, NewFn(func(state *State) Fn {
		Assert(state.Exists(ScopeKeyCurrentTables))
		if !must {
			if RandomBool() {
				return Empty()
			}
		}
		return And(
			Str(","),
			RepeatRange(1, more, IndexDefinition, Str(",")),
		)
	}))
}

func setColumnCountHint(repl *FnHookReplacer, countHint int) {
	Assert(countHint >= 1)
	repl.Replace(ColumnDefinitions, NewFn(func(state *State) Fn {
		Assert(state.Exists(ScopeKeyCurrentTables))
		if !state.Initialized() {
			return Repeat(ColumnDefinition, state.ctrl.InitColCount, Str(","))
		}
		return RepeatRange(1, countHint, ColumnDefinition, Str(","))
	}))
}

func mapConfigKey(s *State) {
	w := s.ctrl.Weight
	if w.Query_INDEX_MERGE {
		s.StoreConfig(ConfigKeyUnitFirstColumnIndexable, NewScopeObj(struct{}{}))
		s.StoreConfig(ConfigKeyUnitIndexMergeHint, NewScopeObj(struct{}{}))
	}
	if w.CreateTable_WithClusterHint {
		s.StoreConfig(ConfigKeyUnitPKNeedClusteredHint, NewScopeObj(struct{}{}))
	}
	if w.CreateTable_MustStrCol {
		s.StoreConfig(ConfigKeyEnumColumnType, NewScopeObj("string"))
	} else if w.CreateTable_MustIntCol {
		s.StoreConfig(ConfigKeyEnumColumnType, NewScopeObj("int"))
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
}
