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
		// Check if there is a next leaf node
		nextLeaf := *leafNodeNextLeaf(page)
		if nextLeaf == 0 {
			c.endOfTable = true
		} else {
			c.pageNum = nextLeaf
			c.cellNum = 0
		}
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
		// TODO: add log to file
		return c.SplitAndInsert(key, value)
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

func (c *Cursor) SplitAndInsert(key uint32, value *Row) error {
	oldPage, err := c.table.pager.getPage(c.pageNum)
	if err != nil {
		return err
	}
	oldPageMaxKey := getNodeMaxKey(oldPage)
	newPageNum := c.table.pager.getUnusedPageNum()
	newPage, err := c.table.pager.getPage(newPageNum)
	if err != nil {
		return err
	}
	initializeLeafNode(newPage)
	*nodeParent(newPage) = *nodeParent(oldPage)
	*leafNodeNextLeaf(newPage) = *leafNodeNextLeaf(oldPage)
	*leafNodeNextLeaf(oldPage) = newPageNum

	// Move half the cells to the new page
	for i := LeafNodeMaxCells; i >= 0; i-- {
		var destPage []byte

		if i >= LeafNodeLeftSplitCount {
			destPage = newPage
		} else {
			destPage = oldPage
		}
		indexWithinPage := i % LeafNodeLeftSplitCount
		dest := leafNodeCell(destPage, uint32(indexWithinPage))

		// Case 1: At the new row's insertion position - write the new row
		// Destination: Either oldPage or newPage (determined by split logic above at line 85-89)
		// Source: The new value parameter being inserted
		if uint32(i) == c.cellNum {
			serializeRow(value,
				leafNodeValue(destPage, uint32(indexWithinPage)))
			*leafNodeKey(destPage, uint32(indexWithinPage)) = key
			// Case 2: After the insertion position - shift existing cells right by 1
			// Destination: Position i in either oldPage or newPage
			// Source: Position (i-1) from oldPage (skipping over where new row will go)
		} else if uint32(i) > c.cellNum {
			copy(dest, leafNodeCell(oldPage, uint32((i-1))))
			// Case 3: Before the insertion position - copy existing cells as-is
			// Destination: Position i in either oldPage or newPage
			// Source: Position i from oldPage (no shift needed for cells before insertion)
		} else {
			copy(dest, leafNodeCell(oldPage, uint32(i)))
		}
	}

	*leafNodeNumCells(oldPage) = uint32(LeafNodeLeftSplitCount)
	*leafNodeNumCells(newPage) = uint32(LeafNodeRightSplitCount)

	if isNodeRoot(oldPage) {
		return c.table.createNewRoot(newPageNum)
	} else {
		parentPageNum := *nodeParent(oldPage)
		parentPage, err := c.table.pager.getPage(parentPageNum)
		if err != nil {
			return err
		}
		newMaxKey := getNodeMaxKey(oldPage)
		updateInternalNodeKey(parentPage, oldPageMaxKey, newMaxKey)
		return c.table.internalNodeInsert(parentPageNum, newPageNum)
	}
}

// TableStart returns a cursor pointing to the start of the table.
func TableStart(table *Table) *Cursor {
	c, err := table.findKey(0)
	if err != nil {
		panic(err)
	}

	page, err := table.pager.getPage(c.pageNum)
	if err != nil {
		panic(err)
	}

	numCells := *leafNodeNumCells(page)
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
