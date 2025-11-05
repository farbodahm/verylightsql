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
var ErrDuplicateKey = errors.New("duplicate key")

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

// getUnusedPageNum returns the next unused page number for appending new pages.
// TODO: This function currently does not handle reusing freed pages after deletions.
func (p *Pager) getUnusedPageNum() uint32 {
	return p.numPages
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
		setNodeRoot(rootNode, true)
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

// findKey finds the position of a key in the table and returns a cursor to it
// if the key is not found, it returns a cursor to the position where it should be inserted
func (t *Table) findKey(key uint32) (*Cursor, error) {
	rootPage, err := t.pager.getPage(t.rootPageNum)
	if err != nil {
		panic(err) // In a real application, handle this error properly
	}

	switch *nodeType(rootPage) {
	case NodeTypeLeaf:
		return t.findKeyInLeaf(t.rootPageNum, key), nil
	case NodeTypeInternal:
		return t.findKeyInInternal(t.rootPageNum, key)
	default:
		return nil, errors.New("unknown node type to find key")
	}
}

// findKeyInLeaf searches for a key in a leaf node and returns a cursor to its position
// if the key is not found, it returns a cursor to the position where it should be inserted
func (t *Table) findKeyInLeaf(pageNum uint32, key uint32) *Cursor {
	node, err := t.pager.getPage(pageNum)
	if err != nil {
		panic(err) // In a real application, handle this error properly
	}
	numOfCells := *leafNodeNumCells(node)
	c := &Cursor{
		table:   t,
		pageNum: pageNum,
	}

	// Binary search
	i, j := uint32(0), numOfCells
	for i != j {
		mid := (i + j) / 2
		midKey := *leafNodeKey(node, mid)
		if key == midKey {
			c.cellNum = mid
			return c
		}
		if key < midKey {
			j = mid
		} else {
			i = mid + 1
		}
	}

	c.cellNum = i
	return c
}

// findKeyInInternal searches for a key in an internal node and returns a cursor to its position
// if the key is not found, it returns a cursor to the position where it should be inserted
func (t *Table) findKeyInInternal(pageNum uint32, key uint32) (*Cursor, error) {
	node, err := t.pager.getPage(pageNum)
	if err != nil {
		return nil, err
	}

	childIndex := internalNodeFindChild(node, key)
	childPageNum := *internalNodeChild(node, childIndex)

	childNode, err := t.pager.getPage(childPageNum)
	if err != nil {
		return nil, err
	}

	switch *nodeType(childNode) {
	case NodeTypeLeaf:
		return t.findKeyInLeaf(childPageNum, key), nil
	case NodeTypeInternal:
		return t.findKeyInInternal(childPageNum, key)
	default:
		return nil, errors.New("unknown node type in internal node child")
	}
}

// createNewRoot creates a new root node when the current root is split
// Old root copied to new page becomes left child.
// New root node becomes the root of the tree.
// Add of the right child is passed as argument.
func (t *Table) createNewRoot(rightChildPageNum uint32) error {
	oldRootPage, err := t.pager.getPage(t.rootPageNum)
	if err != nil {
		return err
	}

	rightChild, err := t.pager.getPage(rightChildPageNum)
	if err != nil {
		return err
	}
	leftChildPageNum := t.pager.getUnusedPageNum()
	leftChild, err := t.pager.getPage(leftChildPageNum)
	if err != nil {
		return err
	}

	// left child has old root's data
	copy(leftChild, oldRootPage)
	setNodeRoot(leftChild, false)

	// root node is a new internal node with one key and two children
	initializeInternalNode(oldRootPage)
	setNodeRoot(oldRootPage, true)
	*internalNodeNumKeys(oldRootPage) = 1
	*internalNodeChild(oldRootPage, 0) = leftChildPageNum
	*internalNodeKey(oldRootPage, 0) = *leafNodeKey(rightChild, 0)
	*internalNodeRightChild(oldRootPage) = rightChildPageNum
	*nodeParent(leftChild) = t.rootPageNum
	*nodeParent(rightChild) = t.rootPageNum
	return nil
}

func (t *Table) internalNodeInsert(parentPageNum uint32, childPageNum uint32) error {
	parentPage, err := t.pager.getPage(parentPageNum)
	if err != nil {
		return err
	}
	numKeys := *internalNodeNumKeys(parentPage)
	if numKeys >= InternalNodeMaxKeys {
		return errors.New("internal node splitting not implemented")
	}
	*internalNodeNumKeys(parentPage) = numKeys + 1

	childPage, err := t.pager.getPage(childPageNum)
	if err != nil {
		return err
	}
	childMaxKey := getNodeMaxKey(childPage)
	index := internalNodeFindChild(parentPage, childMaxKey)

	rightChildPageNum := *internalNodeRightChild(parentPage)
	rightChildPage, err := t.pager.getPage(rightChildPageNum)
	if err != nil {
		return err
	}

	if childMaxKey > getNodeMaxKey(rightChildPage) {
		// Replace right child
		*internalNodeChild(parentPage, numKeys) = rightChildPageNum
		*internalNodeKey(parentPage, numKeys) = getNodeMaxKey(rightChildPage)
		*internalNodeRightChild(parentPage) = childPageNum
	} else {
		// Shift cells to make room for new child
		for i := numKeys; i > index; i-- {
			*internalNodeChild(parentPage, i) = *internalNodeChild(parentPage, i-1)
			*internalNodeKey(parentPage, i) = *internalNodeKey(parentPage, i-1)
		}
		*internalNodeChild(parentPage, index) = childPageNum
		*internalNodeKey(parentPage, index) = childMaxKey
	}

	return nil
}

// Insert adds a new row to the table
func (t *Table) Insert(row *Row) error {
	page, err := t.pager.getPage(t.rootPageNum)
	if err != nil {
		return err
	}

	numOfCells := *leafNodeNumCells(page)

	keyToInsert := uint32(row.ID)
	cursor, err := t.findKey(keyToInsert)
	if err != nil {
		return err
	}

	// Check for duplicate keys
	// Only compare when the cursor points to an existing cell; if it’s at numOfCells,
	// the key wasn’t found and the cursor sits on the first free slot for insertion.
	if cursor.cellNum < numOfCells {
		existingKey := *leafNodeKey(page, cursor.cellNum)
		if existingKey == keyToInsert {
			return ErrDuplicateKey
		}
	}

	return cursor.InsertLeafNode(uint32(row.ID), row)
}

// SelectAll returns all rows in the table
func (t *Table) SelectAll() []Row {
	cursor := TableStart(t)
	rows := make([]Row, 0, t.pager.numPages*uint32(LeafNodeMaxCells))
	var row Row
	for !cursor.IsEndOfTable() {
		deserializeRow(cursor.Value(), &row)
		rows = append(rows, row)
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
