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
	repl.Replace(partitionDef, NewFn(func(state *State) Fn {
		tbl := state.Search(ScopeKeyCurrentTable).ToTable()
		partCol := state.Search(ScopeKeyCurrentPartitionColumn).ToColumn()
		tbl.AppendPartitionColumn(partCol)
		switch tp {
		case "hash":
			return partitionDefHash
		case "range":
			return partitionDefRange
		case "list":
			return partitionDefList
		}
		return Empty()
	}))
}

func setIndexColumnCountHint(repl *FnHookReplacer, more int, must bool) {
	Assert(more >= 1)
	repl.Replace(idxDefs, NewFn(func(state *State) Fn {
		Assert(state.Exists(ScopeKeyCurrentTable))
		if !must {
			if RandomBool() {
				return Empty()
			}
		}
		return And(
			Str(","),
			RepeatRange(1, more, idxDef, Str(",")),
		)
	}))
}

func setColumnCountHint(repl *FnHookReplacer, countHint int) {
	Assert(countHint >= 1)
	repl.Replace(colDefs, NewFn(func(state *State) Fn {
		Assert(!state.Search(ScopeKeyCurrentTable).IsNil())
		if !state.Initialized() {
			return Repeat(colDef, state.ctrl.InitColCount, Str(","))
		}
		return RepeatRange(1, countHint, colDef, Str(","))
	}))
}

func mapConfigKey(s *State) {
	w := s.ctrl.Weight
	if w.Query_INDEX_MERGE {
		s.StoreConfig(ConfigKeyUnitFirstColumnIndexable, NewScopeObj(struct{}{}))
	}
	if w.CreateTable_WithClusterHint {
		s.StoreConfig(ConfigKeyUnitPKNeedClusteredHint, NewScopeObj(struct{}{}))
	}
	if w.CreateTable_MustStrCol {
		s.StoreConfig(ConfigKeyUnitColumnType, NewScopeObj("string"))
	} else if w.CreateTable_MustIntCol {
		s.StoreConfig(ConfigKeyUnitColumnType, NewScopeObj("int"))
	}
}
