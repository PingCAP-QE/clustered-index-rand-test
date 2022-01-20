package sqlgen

import "math/rand"

var PartitionDefinition = NewFn(func(state *State) Fn {
	if state.env.PartColumn == nil {
		return Empty
	}
	return Or(
		Empty,
		PartitionDefinitionHash,
		PartitionDefinitionRange,
		PartitionDefinitionList,
	)
})

var PartitionDefinitionHash = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	partitionNum := RandomNum(1, 6)
	return And(
		Str("partition by hash ("),
		Str(partitionedCol.Name),
		Str(")"),
		Str("partitions"),
		Str(partitionNum),
	)
})

var PartitionDefinitionRange = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	partitionCount := rand.Intn(5) + 1
	vals := partitionedCol.RandomValuesAsc(partitionCount)
	if rand.Intn(2) == 0 {
		partitionCount++
		vals = append(vals, "maxvalue")
	}
	return Strs(
		"partition by range (",
		partitionedCol.Name, ") (",
		PrintRangePartitionDefs(vals),
		")",
	)
})

var PartitionDefinitionList = NewFn(func(state *State) Fn {
	partitionedCol := state.env.PartColumn
	listVals := partitionedCol.RandomValuesAsc(20)
	listGroups := RandomGroups(listVals, rand.Intn(3)+1)
	return Strs(
		"partition by list (",
		partitionedCol.Name, ") (",
		PrintListPartitionDefs(listGroups),
		")",
	)
})
