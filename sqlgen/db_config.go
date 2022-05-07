package sqlgen

type ConfigurableState State

func (s *ConfigurableState) SetMaxTable(count int) {
	NoTooMuchTables = func(s *State) bool {
		return len(s.Tables) < count
	}
}
