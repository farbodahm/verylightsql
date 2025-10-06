package main

// Cursor represents a cursor for iterating over rows in the table.
type Cursor struct {
	rowNum     uint32
	table      *Table
	endOfTable bool
}

// Advance moves the cursor to the next row in the table.
func (c *Cursor) Advance() {
	c.rowNum++
	if c.rowNum >= c.table.NumRows() {
		c.endOfTable = true
	}
}

// Value returns a pointer to the position described by the cursor.
func (c *Cursor) Value() []byte {
	pageNum := c.rowNum / rowsPerPage
	page, err := c.table.pager.getPage(pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}

	rowOffset := c.rowNum % rowsPerPage
	byteOffset := int(rowOffset) * rowSize
	return page[byteOffset : byteOffset+rowSize]
}

func (c *Cursor) IsEndOfTable() bool {
	return c.endOfTable
}

func (c *Cursor) RowNum() uint32 {
	return c.rowNum
}

// TableStart returns a cursor pointing to the start of the table.
func TableStart(table *Table) *Cursor {
	return &Cursor{rowNum: 0, table: table, endOfTable: table.NumRows() == 0}
}

// TableEnd returns a cursor pointing to the end of the table.
func TableEnd(table *Table) *Cursor {
	return &Cursor{rowNum: table.NumRows(), table: table, endOfTable: true}
}
