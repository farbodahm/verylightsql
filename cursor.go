package main

// Cursor represents a cursor for iterating over rows in the table.
type Cursor struct {
	pageNum    uint32
	cellNum    uint32
	table      *Table
	endOfTable bool
}

// Advance moves the cursor to the next row in the table.
func (c *Cursor) Advance() {
	pageNum := c.pageNum
	page, err := c.table.pager.getPage(pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	c.cellNum++
	numCells := *leafNodeNumCells(page)
	if c.cellNum >= numCells {
		c.endOfTable = true
	}

}

// Value returns a pointer to the position described by the cursor.
func (c *Cursor) Value() []byte {
	pageNum := c.pageNum
	page, err := c.table.pager.getPage(pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	return leafNodeValue(page, c.cellNum)
}

func (c *Cursor) IsEndOfTable() bool {
	return c.endOfTable
}

func (c *Cursor) InsertLeafNode(key uint32, value *Row) error {
	page, err := c.table.pager.getPage(c.pageNum)
	if err != nil {
		return err
	}

	numCells := *leafNodeNumCells(page)
	if numCells >= uint32(LeafNodeMaxCells) {
		return ErrLeafSplittingNotImplemented
	}

	if c.cellNum < numCells {
		// Make room for new cell by shifting cells to the right
		for i := numCells; i > c.cellNum; i-- {
			dest := leafNodeCell(page, i)
			src := leafNodeCell(page, i-1)
			copy(dest, src)
		}
	}

	*leafNodeNumCells(page) = numCells + 1
	*leafNodeKey(page, c.cellNum) = key
	serializeRow(value, leafNodeValue(page, c.cellNum))

	return nil
}

// TableStart returns a cursor pointing to the start of the table.
func TableStart(table *Table) *Cursor {
	c := &Cursor{
		pageNum: table.rootPageNum,
		cellNum: 0,
		table:   table,
	}

	rootNode, err := table.pager.getPage(c.pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	numCells := *leafNodeNumCells(rootNode)
	if numCells == 0 {
		c.endOfTable = true
	}

	return c
}

// TableEnd returns a cursor pointing to the end of the table.
func TableEnd(table *Table) *Cursor {
	c := &Cursor{
		pageNum:    table.rootPageNum,
		table:      table,
		endOfTable: true,
	}

	rootNode, err := table.pager.getPage(c.pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	numCells := *leafNodeNumCells(rootNode)
	c.cellNum = numCells
	return c
}
