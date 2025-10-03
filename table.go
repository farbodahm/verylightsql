package main

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
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
var ErrFlushEmptyPage = errors.New("attempt to flush empty page")

// Pager manages the paged file storage
type Pager struct {
	fileLength int64
	file       *os.File
	pages      [tableMaxPages][]byte
}

// getPage retrieves a page from the pager, loading it from disk if necessary.
// It assume pages are saved one after the other in the database file:
// Page 0 at offset 0, page 1 at offset 4096, page 2 at offset 8192, etc.
func (p *Pager) getPage(pageNum uint32) ([]byte, error) {
	if pageNum >= tableMaxPages {
		return nil, errors.New("page number out of bounds")
	}

	// Load page from file if not already loaded
	if p.pages[pageNum] == nil {
		numPages := uint32(p.fileLength / pageSize)
		// We might save a partial page at the end of the file
		if p.fileLength%pageSize != 0 {
			numPages++
		}

		if pageNum <= numPages {
			p.pages[pageNum] = make([]byte, pageSize)
			n, err := p.file.ReadAt(p.pages[pageNum], int64(pageNum)*pageSize)
			if err != nil && err != io.EOF {
				return nil, err
			}
			// If we read less than a full page, zero out the rest
			if n < pageSize {
				for i := n; i < pageSize; i++ {
					p.pages[pageNum][i] = 0
				}
			}
		}
		// TODO: Still can be empty

	}

	return p.pages[pageNum], nil
}

// flush writes a page back to disk
func (p *Pager) flush(pageNum uint32, size uint32) error {
	if p.pages[pageNum] == nil {
		return ErrFlushEmptyPage
	}

	_, err := p.file.WriteAt(p.pages[pageNum][:size], int64(pageNum)*pageSize)
	return err
}

func openPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	pager := &Pager{
		fileLength: fileInfo.Size(),
		file:       file,
	}

	// TODO: Eager allocation of pages can be done here if needed
	return pager, nil
}

// Table represents a database table with paged storage
type Table struct {
	numRows uint32
	pager   *Pager
}

func OpenDatabase(filename string) (*Table, error) {
	pager, err := openPager(filename)
	if err != nil {
		return nil, err
	}

	table := &Table{
		numRows: uint32(pager.fileLength / int64(rowSize)),
		pager:   pager,
	}

	return table, nil
}

// rowSlot returns a pointer to the memory location where a row should be stored
func (t *Table) rowSlot(rowNum uint32) []byte {
	pageNum := rowNum / rowsPerPage

	page, err := t.pager.getPage(pageNum)
	if err != nil {
		panic(err) // TODO: In production code, handle this error properly
	}
	rowOffset := rowNum % rowsPerPage
	byteOffset := int(rowOffset) * rowSize
	return page[byteOffset : byteOffset+rowSize]
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

func (t *Table) Close() error {
	p := t.pager
	fullPagesNum := t.numRows / rowsPerPage

	// Write all pages to disk
	for pageNum := range fullPagesNum {
		if p.pages[pageNum] == nil {
			continue
		}

		err := t.pager.flush(uint32(pageNum), pageSize)
		if err != nil {
			return err
		}
	}
	// Write additional rows of last page if any exists
	additionalRowsNum := t.numRows % rowsPerPage
	if additionalRowsNum > 0 {
		pageNum := fullPagesNum

		err := t.pager.flush(pageNum, additionalRowsNum*uint32(rowSize))
		if err != nil {
			return err
		}
	}

	err := p.file.Close()
	if err != nil {
		return err
	}

	return nil
}
