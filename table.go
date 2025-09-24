package main

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	idSize         = int(unsafe.Sizeof(int32(0)))
	usernameSize   = ColumnUsernameSize
	emailSize      = ColumnEmailSize
	idOffset       = 0
	usernameOffset = idOffset + idSize
	emailOffset    = usernameOffset + usernameSize
	rowSize        = idSize + usernameSize + emailSize

	pageSize      = 4096
	tableMaxPages = 100
	rowsPerPage   = uint32(pageSize / rowSize)
	tableMaxRows  = uint32(rowsPerPage * tableMaxPages)
)

var ErrTableFull = errors.New("table is full")

// Table represents a database table with paged storage
type Table struct {
	numRows uint32
	pages   [tableMaxPages][]byte
}

// NewTable creates a new empty table
func NewTable() *Table {
	return &Table{
		numRows: 0,
	}
}

// rowSlot returns a pointer to the memory location where a row should be stored
func (t *Table) rowSlot(rowNum uint32) []byte {
	pageNum := rowNum / rowsPerPage

	// Allocate page if it doesn't exist
	if t.pages[pageNum] == nil {
		t.pages[pageNum] = make([]byte, pageSize)
	}

	rowOffset := rowNum % rowsPerPage
	byteOffset := int(rowOffset) * rowSize
	return t.pages[pageNum][byteOffset : byteOffset+rowSize]
}

// serializeRow converts a Row struct to bytes and stores it in the destination
func serializeRow(row *Row, dest []byte) {
	binary.LittleEndian.PutUint32(dest[idOffset:], uint32(row.ID))
	copy(dest[usernameOffset:usernameOffset+usernameSize], row.Username[:])
	copy(dest[emailOffset:emailOffset+emailSize], row.Email[:])
}

// deserializeRow converts bytes back to a Row struct
func deserializeRow(src []byte, row *Row) {
	row.ID = int32(binary.LittleEndian.Uint32(src[idOffset:]))
	copy(row.Username[:], src[usernameOffset:usernameOffset+usernameSize])
	copy(row.Email[:], src[emailOffset:emailOffset+emailSize])
}

// Insert adds a new row to the table
func (t *Table) Insert(row *Row) error {
	if t.numRows >= tableMaxRows {
		return ErrTableFull
	}

	slot := t.rowSlot(t.numRows)
	serializeRow(row, slot)
	t.numRows++

	return nil
}

// SelectAll returns all rows in the table
func (t *Table) SelectAll() []Row {
	rows := make([]Row, t.numRows)

	for i := uint32(0); i < t.numRows; i++ {
		slot := t.rowSlot(i)
		deserializeRow(slot, &rows[i])
	}

	return rows
}

// NumRows returns the current number of rows in the table
func (t *Table) NumRows() uint32 {
	return t.numRows
}
