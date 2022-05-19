package sqlgen

type IDAllocator struct {
	tableID  int
	columnID int
	indexID  int
	cteID    int
	renameID int
}

func (a *IDAllocator) AllocTableID() int {
	a.tableID++
	return a.tableID
}

func (a *IDAllocator) AllocColumnID() int {
	a.columnID++
	return a.columnID
}

func (a *IDAllocator) AllocIndexID() int {
	a.indexID++
	return a.indexID
}

func (a *IDAllocator) AllocCTEID() int {
	a.cteID++
	return a.cteID
}

func (a *IDAllocator) AllocRenameID() int {
	a.renameID++
	return a.renameID
}
