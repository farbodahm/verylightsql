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
	node, err := c.table.pager.getLeafNode(c.pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	c.cellNum++
	if c.cellNum >= node.NumCells {
		// Check if there is a next leaf node
		if node.NextLeaf == 0 {
			c.endOfTable = true
		} else {
			c.pageNum = node.NextLeaf
			c.cellNum = 0
		}
	}
}

// Value returns the row at the current cursor position.
func (c *Cursor) Value() Row {
	node, err := c.table.pager.getLeafNode(c.pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	return node.Cells[c.cellNum].Value
}

func (c *Cursor) IsEndOfTable() bool {
	return c.endOfTable
}

func (c *Cursor) InsertLeafNode(key uint32, value *Row) error {
	node, err := c.table.pager.getLeafNode(c.pageNum)
	if err != nil {
		return err
	}

	if node.NumCells >= uint32(LeafNodeMaxCells) {
		return c.SplitAndInsert(key, value)
	}

	if c.cellNum < node.NumCells {
		// Make room for new cell by shifting cells to the right
		for i := node.NumCells; i > c.cellNum; i-- {
			node.Cells[i] = node.Cells[i-1]
		}
	}

	node.NumCells++
	node.Cells[c.cellNum].Key = key
	node.Cells[c.cellNum].Value = *value

	return c.table.pager.saveLeafNode(c.pageNum, node)
}

func (c *Cursor) SplitAndInsert(key uint32, value *Row) error {
	oldNode, err := c.table.pager.getLeafNode(c.pageNum)
	if err != nil {
		return err
	}

	newPageNum := c.table.pager.getUnusedPageNum()
	newNode := NewLeafNode()
	newNode.Parent = oldNode.Parent
	newNode.NextLeaf = oldNode.NextLeaf
	oldNode.NextLeaf = newPageNum

	// Move half the cells to the new page
	// We need to distribute (LeafNodeMaxCells + 1) cells into:
	// - oldNode: LeafNodeLeftSplitCount cells (indices 0 to LeafNodeLeftSplitCount-1)
	// - newNode: LeafNodeRightSplitCount cells (indices 0 to LeafNodeRightSplitCount-1)

	// First, collect all cells including the new one
	type cellData struct {
		key   uint32
		value Row
	}
	allCells := make([]cellData, LeafNodeMaxCells+1)

	// Copy existing cells and insert new one at the right position
	insertIdx := 0
	for i := uint32(0); i <= uint32(LeafNodeMaxCells); i++ {
		if i == c.cellNum {
			allCells[i] = cellData{key: key, value: *value}
		} else {
			srcIdx := insertIdx
			if insertIdx >= int(oldNode.NumCells) {
				// This shouldn't happen if our logic is correct
				break
			}
			allCells[i] = cellData{
				key:   oldNode.Cells[srcIdx].Key,
				value: oldNode.Cells[srcIdx].Value,
			}
			insertIdx++
		}
	}

	// Redistribute cells
	for i := 0; i < LeafNodeLeftSplitCount; i++ {
		oldNode.Cells[i].Key = allCells[i].key
		oldNode.Cells[i].Value = allCells[i].value
	}
	oldNode.NumCells = uint32(LeafNodeLeftSplitCount)

	for i := 0; i < LeafNodeRightSplitCount; i++ {
		srcIdx := LeafNodeLeftSplitCount + i
		newNode.Cells[i].Key = allCells[srcIdx].key
		newNode.Cells[i].Value = allCells[srcIdx].value
	}
	newNode.NumCells = uint32(LeafNodeRightSplitCount)

	// Save both nodes
	if err := c.table.pager.saveLeafNode(c.pageNum, oldNode); err != nil {
		return err
	}
	if err := c.table.pager.saveLeafNode(newPageNum, newNode); err != nil {
		return err
	}

	if oldNode.IsRootNode() {
		return c.table.createNewRoot(newPageNum)
	} else {
		parentPageNum := oldNode.Parent
		parentNode, err := c.table.pager.getInternalNode(parentPageNum)
		if err != nil {
			return err
		}
		// Find the child index by page number (more reliable than by key)
		oldChildIndex := parentNode.FindChildByPage(c.pageNum)
		// Update the key for this child (only if it's not the rightmost child)
		if oldChildIndex < parentNode.NumKeys {
			parentNode.Cells[oldChildIndex].Key = oldNode.GetMaxKey()
		}
		if err := c.table.pager.saveInternalNode(parentPageNum, parentNode); err != nil {
			return err
		}
		return c.table.internalNodeInsert(parentPageNum, newPageNum)
	}
}

// TableStart returns a cursor pointing to the start of the table.
func TableStart(table *Table) *Cursor {
	c, err := table.findKey(0)
	if err != nil {
		panic(err)
	}

	node, err := table.pager.getLeafNode(c.pageNum)
	if err != nil {
		panic(err)
	}

	if node.NumCells == 0 {
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

	rootNode, err := table.pager.getLeafNode(c.pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	c.cellNum = rootNode.NumCells
	return c
}
