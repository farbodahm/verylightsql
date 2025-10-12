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
)

var ErrTableFull = errors.New("table is full")
var ErrFlushEmptyPage = errors.New("attempt to flush empty page")
var ErrLeafSplittingNotImplemented = errors.New("leaf node splitting not implemented")

// Pager manages the paged file storage
type Pager struct {
	fileLength int64
	file       *os.File
	pages      [tableMaxPages][]byte
	numPages   uint32
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

		if p.pages[pageNum] == nil {
			// We still have to allocate a fresh page here because there may be no
			// persisted data for this page yet (e.g. when appending new rows past the
			// current file length), so the caller always receives a writable buffer.
			p.pages[pageNum] = make([]byte, pageSize)
		}

		// Update numPages if we just allocated a new page
		if pageNum >= p.numPages {
			p.numPages = pageNum + 1
		}
	}

	return p.pages[pageNum], nil
}

// flush writes a page back to disk
// Each Btree node is a page, so this function is used to persist Btree nodes
func (p *Pager) flush(pageNum uint32) error {
	if p.pages[pageNum] == nil {
		return ErrFlushEmptyPage
	}

	_, err := p.file.WriteAt(p.pages[pageNum], int64(pageNum)*pageSize)
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

	fileSize := fileInfo.Size()
	if fileSize%pageSize != 0 {
		return nil, errors.New("db file is not a whole number of pages. Corrupt file?")
	}

	pager := &Pager{
		fileLength: fileSize,
		file:       file,
		numPages:   uint32(fileSize / pageSize),
	}

	// TODO: Eager allocation of pages can be done here if needed
	return pager, nil
}

// Table represents a database table with paged storage
type Table struct {
	rootPageNum uint32
	pager       *Pager
}

func OpenDatabase(filename string) (*Table, error) {
	pager, err := openPager(filename)
	if err != nil {
		return nil, err
	}

	table := &Table{
		pager:       pager,
		rootPageNum: 0,
	}

	if pager.numPages == 0 {
		// New database file. Initialize page 0 as leaf node
		rootNode, err := pager.getPage(0)
		if err != nil {
			return nil, err
		}
		initializeLeafNode(rootNode)
	}

	return table, nil
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
	node, err := t.pager.getPage(t.rootPageNum)
	if err != nil {
		return err
	}
	if *leafNodeNumCells(node) >= uint32(LeafNodeMaxCells) {
		return ErrTableFull // For simplicity, we don't handle splitting in this example
	}

	cursor := TableEnd(t) // TODO: optimize by avoiding to create a new cursor each time\
	return cursor.InsertLeafNode(uint32(row.ID), row)
}

// SelectAll returns all rows in the table
func (t *Table) SelectAll() []Row {
	page, _ := t.pager.getPage(t.rootPageNum)
	numOfCells := *leafNodeNumCells(page)
	rows := make([]Row, numOfCells)
	cursor := TableStart(t)

	for !cursor.IsEndOfTable() {
		deserializeRow(cursor.Value(), &rows[cursor.cellNum])
		cursor.Advance()
	}

	return rows
}

func (t *Table) Close() error {
	p := t.pager

	// Write all pages to disk
	for pageNum := range t.pager.numPages {
		if p.pages[pageNum] == nil {
			continue
		}

		err := t.pager.flush(uint32(pageNum))
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
