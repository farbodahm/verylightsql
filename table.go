package main

import (
	"errors"
	"io"
	"os"
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

// getLeafNode retrieves a leaf node from the pager
func (p *Pager) getLeafNode(pageNum uint32) (*LeafNode, error) {
	page, err := p.getPage(pageNum)
	if err != nil {
		return nil, err
	}
	return DeserializeLeafNode(page)
}

// getInternalNode retrieves an internal node from the pager
func (p *Pager) getInternalNode(pageNum uint32) (*InternalNode, error) {
	page, err := p.getPage(pageNum)
	if err != nil {
		return nil, err
	}
	return DeserializeInternalNode(page)
}

// savePage writes a node back to the page cache
func (p *Pager) saveLeafNode(pageNum uint32, node *LeafNode) error {
	if pageNum >= tableMaxPages {
		return errors.New("page number out of bounds")
	}
	page, err := node.Serialize()
	if err != nil {
		return err
	}
	p.pages[pageNum] = page
	// Update numPages if we just allocated a new page
	if pageNum >= p.numPages {
		p.numPages = pageNum + 1
	}
	return nil
}

// saveInternalNode writes an internal node back to the page cache
func (p *Pager) saveInternalNode(pageNum uint32, node *InternalNode) error {
	if pageNum >= tableMaxPages {
		return errors.New("page number out of bounds")
	}
	page, err := node.Serialize()
	if err != nil {
		return err
	}
	p.pages[pageNum] = page
	// Update numPages if we just allocated a new page
	if pageNum >= p.numPages {
		p.numPages = pageNum + 1
	}
	return nil
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
		rootNode := NewLeafNode()
		rootNode.SetRoot(true)
		if err := pager.saveLeafNode(0, rootNode); err != nil {
			return nil, err
		}
		pager.numPages = 1
	}

	return table, nil
}

// findKey finds the position of a key in the table and returns a cursor to it
// if the key is not found, it returns a cursor to the position where it should be inserted
func (t *Table) findKey(key uint32) (*Cursor, error) {
	page, err := t.pager.getPage(t.rootPageNum)
	if err != nil {
		return nil, err
	}

	switch GetNodeTypeFromPage(page) {
	case NodeTypeLeaf:
		return t.findKeyInLeaf(t.rootPageNum, key)
	case NodeTypeInternal:
		return t.findKeyInInternal(t.rootPageNum, key)
	default:
		return nil, errors.New("unknown node type to find key")
	}
}

// findKeyInLeaf searches for a key in a leaf node and returns a cursor to its position
// if the key is not found, it returns a cursor to the position where it should be inserted
func (t *Table) findKeyInLeaf(pageNum uint32, key uint32) (*Cursor, error) {
	node, err := t.pager.getLeafNode(pageNum)
	if err != nil {
		return nil, err
	}

	c := &Cursor{
		table:   t,
		pageNum: pageNum,
	}

	// Binary search
	i, j := uint32(0), node.NumCells
	for i != j {
		mid := (i + j) / 2
		midKey := node.Cells[mid].Key
		if key == midKey {
			c.cellNum = mid
			return c, nil
		}
		if key < midKey {
			j = mid
		} else {
			i = mid + 1
		}
	}

	c.cellNum = i
	return c, nil
}

// findKeyInInternal searches for a key in an internal node and returns a cursor to its position
// if the key is not found, it returns a cursor to the position where it should be inserted
func (t *Table) findKeyInInternal(pageNum uint32, key uint32) (*Cursor, error) {
	node, err := t.pager.getInternalNode(pageNum)
	if err != nil {
		return nil, err
	}

	childIndex := node.FindChild(key)
	childPageNum := node.GetChild(childIndex)

	childPage, err := t.pager.getPage(childPageNum)
	if err != nil {
		return nil, err
	}

	switch GetNodeTypeFromPage(childPage) {
	case NodeTypeLeaf:
		return t.findKeyInLeaf(childPageNum, key)
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
	oldNodeType := GetNodeTypeFromPage(oldRootPage)

	rightChildPage, err := t.pager.getPage(rightChildPageNum)
	if err != nil {
		return err
	}

	leftChildPageNum := t.pager.getUnusedPageNum()

	// Copy old root to left child
	leftChildPage := make([]byte, pageSize)
	copy(leftChildPage, oldRootPage)
	t.pager.pages[leftChildPageNum] = leftChildPage
	if leftChildPageNum >= t.pager.numPages {
		t.pager.numPages = leftChildPageNum + 1
	}

	// Update left child's IsRoot flag
	leftChildPage[1] = 0 // IsRoot = false

	// Get max key from left child
	leftMaxKey, err := GetMaxKeyFromPage(leftChildPage)
	if err != nil {
		return err
	}

	// Create new root as internal node
	newRoot := NewInternalNode()
	newRoot.SetRoot(true)
	newRoot.NumKeys = 1
	newRoot.Cells[0].Child = leftChildPageNum
	newRoot.Cells[0].Key = leftMaxKey
	newRoot.RightChild = rightChildPageNum

	// Save new root
	if err := t.pager.saveInternalNode(t.rootPageNum, newRoot); err != nil {
		return err
	}

	// Update parent pointers for children
	// Left child
	if oldNodeType == NodeTypeLeaf {
		leftNode, err := DeserializeLeafNode(leftChildPage)
		if err != nil {
			return err
		}
		leftNode.Parent = t.rootPageNum
		if err := t.pager.saveLeafNode(leftChildPageNum, leftNode); err != nil {
			return err
		}
	} else {
		leftNode, err := DeserializeInternalNode(leftChildPage)
		if err != nil {
			return err
		}
		leftNode.Parent = t.rootPageNum
		if err := t.pager.saveInternalNode(leftChildPageNum, leftNode); err != nil {
			return err
		}
		// Update grandchildren parent pointers
		for i := uint32(0); i <= leftNode.NumKeys; i++ {
			grandchildPageNum := leftNode.GetChild(i)
			grandchildPage, err := t.pager.getPage(grandchildPageNum)
			if err != nil {
				return err
			}
			if GetNodeTypeFromPage(grandchildPage) == NodeTypeLeaf {
				grandchild, err := DeserializeLeafNode(grandchildPage)
				if err != nil {
					return err
				}
				grandchild.Parent = leftChildPageNum
				if err := t.pager.saveLeafNode(grandchildPageNum, grandchild); err != nil {
					return err
				}
			} else {
				grandchild, err := DeserializeInternalNode(grandchildPage)
				if err != nil {
					return err
				}
				grandchild.Parent = leftChildPageNum
				if err := t.pager.saveInternalNode(grandchildPageNum, grandchild); err != nil {
					return err
				}
			}
		}
	}

	// Right child
	if GetNodeTypeFromPage(rightChildPage) == NodeTypeLeaf {
		rightNode, err := DeserializeLeafNode(rightChildPage)
		if err != nil {
			return err
		}
		rightNode.Parent = t.rootPageNum
		if err := t.pager.saveLeafNode(rightChildPageNum, rightNode); err != nil {
			return err
		}
	} else {
		rightNode, err := DeserializeInternalNode(rightChildPage)
		if err != nil {
			return err
		}
		rightNode.Parent = t.rootPageNum
		if err := t.pager.saveInternalNode(rightChildPageNum, rightNode); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) internalNodeInsert(parentPageNum uint32, childPageNum uint32) error {
	parentNode, err := t.pager.getInternalNode(parentPageNum)
	if err != nil {
		return err
	}

	if parentNode.NumKeys >= InternalNodeMaxKeys {
		// Need to split the internal node
		return t.internalNodeSplitAndInsert(parentPageNum, childPageNum)
	}

	childPage, err := t.pager.getPage(childPageNum)
	if err != nil {
		return err
	}
	childMaxKey, err := GetMaxKeyFromPage(childPage)
	if err != nil {
		return err
	}

	rightChildPage, err := t.pager.getPage(parentNode.RightChild)
	if err != nil {
		return err
	}
	rightChildMaxKey, err := GetMaxKeyFromPage(rightChildPage)
	if err != nil {
		return err
	}

	if childMaxKey > rightChildMaxKey {
		// New child becomes the rightmost child
		// Move current right child to become a regular cell
		parentNode.Cells[parentNode.NumKeys].Child = parentNode.RightChild
		parentNode.Cells[parentNode.NumKeys].Key = rightChildMaxKey
		parentNode.RightChild = childPageNum
	} else {
		// Find where to insert the new child
		index := parentNode.FindChild(childMaxKey)
		// Shift cells to make room for new child
		for i := parentNode.NumKeys; i > index; i-- {
			parentNode.Cells[i] = parentNode.Cells[i-1]
		}
		parentNode.Cells[index].Child = childPageNum
		parentNode.Cells[index].Key = childMaxKey
	}

	// Increment key count after all modifications
	parentNode.NumKeys++

	return t.pager.saveInternalNode(parentPageNum, parentNode)
}

// internalNodeSplitAndInsert splits an internal node and inserts a new child.
// This is called when an internal node is full and we need to add another child.
func (t *Table) internalNodeSplitAndInsert(oldPageNum uint32, childPageNum uint32) error {
	oldNode, err := t.pager.getInternalNode(oldPageNum)
	if err != nil {
		return err
	}

	childPage, err := t.pager.getPage(childPageNum)
	if err != nil {
		return err
	}
	childMaxKey, err := GetMaxKeyFromPage(childPage)
	if err != nil {
		return err
	}

	// Check if we're splitting the root
	splittingRoot := oldNode.IsRootNode()

	// Get information we need before potentially modifying pages
	oldNumKeys := oldNode.NumKeys
	oldRightChild := oldNode.RightChild
	oldParentPageNum := oldNode.Parent

	curRightChildPage, err := t.pager.getPage(oldRightChild)
	if err != nil {
		return err
	}
	curRightChildMaxKey, err := GetMaxKeyFromPage(curRightChildPage)
	if err != nil {
		return err
	}

	// Determine where the new child should be inserted
	var newChildIndex uint32
	if childMaxKey > curRightChildMaxKey {
		// New child would become the rightmost
		newChildIndex = oldNumKeys + 1
	} else {
		newChildIndex = oldNode.FindChild(childMaxKey)
	}

	// Create temporary storage for all keys and children (including the new one)
	type keyChild struct {
		child uint32
		key   uint32
	}
	allCells := make([]keyChild, InternalNodeMaxKeys+1)
	var allRightChild uint32

	// Collect all existing cells plus the new one in sorted order
	cellIdx := 0
	for i := uint32(0); i <= oldNumKeys; i++ {
		// Insert new child at its position
		if i == newChildIndex {
			allCells[cellIdx] = keyChild{child: childPageNum, key: childMaxKey}
			cellIdx++
		}

		if i < oldNumKeys {
			allCells[cellIdx] = keyChild{
				child: oldNode.Cells[i].Child,
				key:   oldNode.Cells[i].Key,
			}
			cellIdx++
		} else if i == oldNumKeys {
			// Handle the right child
			if newChildIndex == oldNumKeys+1 {
				// New child becomes rightmost
				allCells[cellIdx] = keyChild{child: oldRightChild, key: curRightChildMaxKey}
				cellIdx++
				allRightChild = childPageNum
			} else {
				allRightChild = oldRightChild
			}
		}
	}

	// The key that will go to parent
	parentKey := allCells[InternalNodeLeftSplitCount].key

	// Allocate pages properly - we need to reserve them first
	newPageNum := t.pager.getUnusedPageNum()
	t.pager.numPages = newPageNum + 1 // Reserve this page
	newNode := NewInternalNode()

	if splittingRoot {
		// We need to create a new root first, then set up both children
		leftChildPageNum := t.pager.getUnusedPageNum() // Now this returns newPageNum + 1
		t.pager.numPages = leftChildPageNum + 1       // Reserve this page
		leftNode := NewInternalNode()

		// Set up left node (copy of old root's content, redistributed)
		leftNode.NumKeys = uint32(InternalNodeLeftSplitCount)
		for i := 0; i < InternalNodeLeftSplitCount; i++ {
			leftNode.Cells[i].Child = allCells[i].child
			leftNode.Cells[i].Key = allCells[i].key
		}
		leftNode.RightChild = allCells[InternalNodeLeftSplitCount].child
		leftNode.Parent = t.rootPageNum

		// Set up right node
		newNode.NumKeys = uint32(InternalNodeRightSplitCount)
		for i := 0; i < InternalNodeRightSplitCount; i++ {
			srcIdx := InternalNodeLeftSplitCount + 1 + i
			newNode.Cells[i].Child = allCells[srcIdx].child
			newNode.Cells[i].Key = allCells[srcIdx].key
		}
		newNode.RightChild = allRightChild
		newNode.Parent = t.rootPageNum

		// Set up new root
		newRoot := NewInternalNode()
		newRoot.SetRoot(true)
		newRoot.NumKeys = 1
		newRoot.Cells[0].Child = leftChildPageNum
		newRoot.Cells[0].Key = parentKey
		newRoot.RightChild = newPageNum

		// Save all nodes
		if err := t.pager.saveInternalNode(t.rootPageNum, newRoot); err != nil {
			return err
		}
		if err := t.pager.saveInternalNode(leftChildPageNum, leftNode); err != nil {
			return err
		}
		if err := t.pager.saveInternalNode(newPageNum, newNode); err != nil {
			return err
		}

		// Update parent pointers for all grandchildren
		// Children that go to leftNode
		for i := uint32(0); i <= leftNode.NumKeys; i++ {
			grandchildPageNum := leftNode.GetChild(i)
			if err := t.updateChildParent(grandchildPageNum, leftChildPageNum); err != nil {
				return err
			}
		}
		// Children that go to newNode
		for i := uint32(0); i <= newNode.NumKeys; i++ {
			grandchildPageNum := newNode.GetChild(i)
			if err := t.updateChildParent(grandchildPageNum, newPageNum); err != nil {
				return err
			}
		}

		return nil
	}

	// Non-root split: update old page in place, create new sibling
	newNode.Parent = oldParentPageNum

	// Update old (left) node
	oldNode.NumKeys = uint32(InternalNodeLeftSplitCount)
	for i := 0; i < InternalNodeLeftSplitCount; i++ {
		oldNode.Cells[i].Child = allCells[i].child
		oldNode.Cells[i].Key = allCells[i].key
	}
	oldNode.RightChild = allCells[InternalNodeLeftSplitCount].child

	// Update new (right) node
	newNode.NumKeys = uint32(InternalNodeRightSplitCount)
	for i := 0; i < InternalNodeRightSplitCount; i++ {
		srcIdx := InternalNodeLeftSplitCount + 1 + i
		newNode.Cells[i].Child = allCells[srcIdx].child
		newNode.Cells[i].Key = allCells[srcIdx].key
	}
	newNode.RightChild = allRightChild

	// Save nodes
	if err := t.pager.saveInternalNode(oldPageNum, oldNode); err != nil {
		return err
	}
	if err := t.pager.saveInternalNode(newPageNum, newNode); err != nil {
		return err
	}

	// Update parent pointers for all children that moved to the new node
	for i := uint32(0); i <= newNode.NumKeys; i++ {
		childPgNum := newNode.GetChild(i)
		if err := t.updateChildParent(childPgNum, newPageNum); err != nil {
			return err
		}
	}

	// Update parent pointers for children in old node (they may have been shuffled)
	for i := uint32(0); i <= oldNode.NumKeys; i++ {
		childPgNum := oldNode.GetChild(i)
		if err := t.updateChildParent(childPgNum, oldPageNum); err != nil {
			return err
		}
	}

	// Update the old key in parent and insert new child
	parentNode, err := t.pager.getInternalNode(oldParentPageNum)
	if err != nil {
		return err
	}

	// Find the child index by page number and update the key
	oldChildIndex := parentNode.FindChildByPage(oldPageNum)
	if oldChildIndex < parentNode.NumKeys {
		oldNodeMaxKey, err := GetMaxKeyFromPage(t.pager.pages[oldPageNum])
		if err != nil {
			return err
		}
		parentNode.Cells[oldChildIndex].Key = oldNodeMaxKey
	}
	if err := t.pager.saveInternalNode(oldParentPageNum, parentNode); err != nil {
		return err
	}

	// Insert the new right sibling into the parent
	// This may recursively split the parent
	return t.internalNodeInsert(oldParentPageNum, newPageNum)
}

// updateChildParent updates the parent pointer for a child node
func (t *Table) updateChildParent(childPageNum uint32, newParentPageNum uint32) error {
	childPage, err := t.pager.getPage(childPageNum)
	if err != nil {
		return err
	}

	if GetNodeTypeFromPage(childPage) == NodeTypeLeaf {
		child, err := DeserializeLeafNode(childPage)
		if err != nil {
			return err
		}
		child.Parent = newParentPageNum
		return t.pager.saveLeafNode(childPageNum, child)
	} else {
		child, err := DeserializeInternalNode(childPage)
		if err != nil {
			return err
		}
		child.Parent = newParentPageNum
		return t.pager.saveInternalNode(childPageNum, child)
	}
}

// Insert adds a new row to the table
func (t *Table) Insert(row *Row) error {
	rootPage, err := t.pager.getPage(t.rootPageNum)
	if err != nil {
		return err
	}

	var numOfCells uint32
	if GetNodeTypeFromPage(rootPage) == NodeTypeLeaf {
		rootNode, err := DeserializeLeafNode(rootPage)
		if err != nil {
			return err
		}
		numOfCells = rootNode.NumCells
	}

	keyToInsert := uint32(row.ID)
	cursor, err := t.findKey(keyToInsert)
	if err != nil {
		return err
	}

	// Check for duplicate keys - we need to re-read the node at cursor position
	cursorNode, err := t.pager.getLeafNode(cursor.pageNum)
	if err != nil {
		return err
	}

	// Only compare when the cursor points to an existing cell
	if cursor.cellNum < cursorNode.NumCells {
		existingKey := cursorNode.Cells[cursor.cellNum].Key
		if existingKey == keyToInsert {
			return ErrDuplicateKey
		}
	}

	// Use numOfCells from the node at cursor position, not root
	_ = numOfCells // Not used since we check at cursor position now

	return cursor.InsertLeafNode(uint32(row.ID), row)
}

// SelectAll returns all rows in the table
func (t *Table) SelectAll() []Row {
	cursor := TableStart(t)
	rows := make([]Row, 0, t.pager.numPages*uint32(LeafNodeMaxCells))

	for !cursor.IsEndOfTable() {
		row := cursor.Value()
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
