package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// NodeType represents the type of a B-tree node.
type NodeType uint8

const (
	NodeTypeInternal NodeType = iota
	NodeTypeLeaf
)

// Page and storage constants
const (
	pageSize      = 4096
	tableMaxPages = 100
)

// Row size constants (must match parser.go)
const (
	rowSize = 4 + ColumnUsernameSize + ColumnEmailSize // 4 (ID) + 32 + 255 = 291
)

// LeafNode constants
const (
	// Header: NodeType(1) + IsRoot(1) + Parent(4) + NumCells(4) + NextLeaf(4) = 14 bytes
	LeafNodeHeaderSize = 14
	// Cell: Key(4) + Row(291) = 295 bytes
	LeafNodeCellSize      = 4 + rowSize
	LeafNodeSpaceForCells = pageSize - LeafNodeHeaderSize
	LeafNodeMaxCells      = LeafNodeSpaceForCells / LeafNodeCellSize // 13
)

// Leaf node split constants
const (
	LeafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2
	LeafNodeLeftSplitCount  = (LeafNodeMaxCells + 1) - LeafNodeRightSplitCount
)

// InternalNode constants
const (
	// Header: NodeType(1) + IsRoot(1) + Parent(4) + NumKeys(4) + RightChild(4) = 14 bytes
	InternalNodeHeaderSize = 14
	// Cell: Child(4) + Key(4) = 8 bytes
	InternalNodeCellSize = 8
	InternalNodeMaxKeys  = 3
)

// Internal node split constants
const (
	InternalNodeRightSplitCount = (InternalNodeMaxKeys + 1) / 2
	InternalNodeLeftSplitCount  = (InternalNodeMaxKeys + 1) - InternalNodeRightSplitCount - 1
)

// LeafCell represents a cell in a leaf node
type LeafCell struct {
	Key   uint32
	Value Row
}

// LeafNode represents a leaf node in the B-tree
type LeafNode struct {
	NodeType uint8
	IsRoot   uint8
	Parent   uint32
	NumCells uint32
	NextLeaf uint32
	Cells    [LeafNodeMaxCells]LeafCell
}

// InternalCell represents a cell in an internal node
type InternalCell struct {
	Child uint32
	Key   uint32
}

// InternalNode represents an internal node in the B-tree
type InternalNode struct {
	NodeType   uint8
	IsRoot     uint8
	Parent     uint32
	NumKeys    uint32
	RightChild uint32
	Cells      [InternalNodeMaxKeys]InternalCell
}

// Serialize writes the LeafNode to a byte slice (page)
func (n *LeafNode) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, n); err != nil {
		return nil, err
	}
	// Pad to page size
	page := make([]byte, pageSize)
	copy(page, buf.Bytes())
	return page, nil
}

// Deserialize reads a LeafNode from a byte slice (page)
func (n *LeafNode) Deserialize(page []byte) error {
	buf := bytes.NewReader(page)
	return binary.Read(buf, binary.LittleEndian, n)
}

// Serialize writes the InternalNode to a byte slice (page)
func (n *InternalNode) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, n); err != nil {
		return nil, err
	}
	// Pad to page size
	page := make([]byte, pageSize)
	copy(page, buf.Bytes())
	return page, nil
}

// Deserialize reads an InternalNode from a byte slice (page)
func (n *InternalNode) Deserialize(page []byte) error {
	buf := bytes.NewReader(page)
	return binary.Read(buf, binary.LittleEndian, n)
}

// Helper functions for creating and initializing nodes

// NewLeafNode creates a new initialized leaf node
func NewLeafNode() *LeafNode {
	return &LeafNode{
		NodeType: uint8(NodeTypeLeaf),
		IsRoot:   0,
		Parent:   0,
		NumCells: 0,
		NextLeaf: 0,
	}
}

// NewInternalNode creates a new initialized internal node
func NewInternalNode() *InternalNode {
	return &InternalNode{
		NodeType:   uint8(NodeTypeInternal),
		IsRoot:     0,
		Parent:     0,
		NumKeys:    0,
		RightChild: 0,
	}
}

// SetRoot sets the IsRoot flag on the node
func (n *LeafNode) SetRoot(isRoot bool) {
	if isRoot {
		n.IsRoot = 1
	} else {
		n.IsRoot = 0
	}
}

// SetRoot sets the IsRoot flag on the node
func (n *InternalNode) SetRoot(isRoot bool) {
	if isRoot {
		n.IsRoot = 1
	} else {
		n.IsRoot = 0
	}
}

// IsRootNode returns whether the node is a root
func (n *LeafNode) IsRootNode() bool {
	return n.IsRoot != 0
}

// IsRootNode returns whether the node is a root
func (n *InternalNode) IsRootNode() bool {
	return n.IsRoot != 0
}

// GetNodeType returns the node type
func (n *LeafNode) GetNodeType() NodeType {
	return NodeType(n.NodeType)
}

// GetNodeType returns the node type
func (n *InternalNode) GetNodeType() NodeType {
	return NodeType(n.NodeType)
}

// GetMaxKey returns the maximum key in the leaf node
func (n *LeafNode) GetMaxKey() uint32 {
	if n.NumCells == 0 {
		panic("cannot get max key from empty leaf node")
	}
	return n.Cells[n.NumCells-1].Key
}

// GetMaxKey returns the maximum key in the internal node
func (n *InternalNode) GetMaxKey() uint32 {
	if n.NumKeys == 0 {
		panic("cannot get max key from empty internal node")
	}
	return n.Cells[n.NumKeys-1].Key
}

// FindChild returns the index of the child pointer which should contain the given key
func (n *InternalNode) FindChild(key uint32) uint32 {
	// Binary search
	i, j := uint32(0), n.NumKeys
	for i != j {
		mid := (i + j) / 2
		midKey := n.Cells[mid].Key
		if key < midKey {
			j = mid
		} else {
			i = mid + 1
		}
	}
	return i
}

// GetChild returns the child page number at the given index
// Returns RightChild if index equals NumKeys
func (n *InternalNode) GetChild(index uint32) uint32 {
	if index > n.NumKeys {
		panic(fmt.Sprintf("Tried to access child_num %d > num_keys %d", index, n.NumKeys))
	}
	if index == n.NumKeys {
		return n.RightChild
	}
	return n.Cells[index].Child
}

// SetChild sets the child page number at the given index
func (n *InternalNode) SetChild(index uint32, pageNum uint32) {
	if index > n.NumKeys {
		panic(fmt.Sprintf("Tried to set child_num %d > num_keys %d", index, n.NumKeys))
	}
	if index == n.NumKeys {
		n.RightChild = pageNum
	} else {
		n.Cells[index].Child = pageNum
	}
}

// FindChildByPage returns the index of the child with the given page number
func (n *InternalNode) FindChildByPage(childPageNum uint32) uint32 {
	for i := uint32(0); i < n.NumKeys; i++ {
		if n.Cells[i].Child == childPageNum {
			return i
		}
	}
	if n.RightChild == childPageNum {
		return n.NumKeys
	}
	panic("child not found in parent")
}

// Utility functions for working with pages

// GetNodeTypeFromPage returns the node type from a raw page
func GetNodeTypeFromPage(page []byte) NodeType {
	return NodeType(page[0])
}

// DeserializeLeafNode creates a LeafNode from a page
func DeserializeLeafNode(page []byte) (*LeafNode, error) {
	node := &LeafNode{}
	if err := node.Deserialize(page); err != nil {
		return nil, err
	}
	return node, nil
}

// DeserializeInternalNode creates an InternalNode from a page
func DeserializeInternalNode(page []byte) (*InternalNode, error) {
	node := &InternalNode{}
	if err := node.Deserialize(page); err != nil {
		return nil, err
	}
	return node, nil
}

// SerializeRow converts a Row struct to bytes
func SerializeRow(row *Row, dest []byte) {
	binary.LittleEndian.PutUint32(dest[0:4], uint32(row.ID))
	copy(dest[4:4+ColumnUsernameSize], row.Username[:])
	copy(dest[4+ColumnUsernameSize:4+ColumnUsernameSize+ColumnEmailSize], row.Email[:])
}

// DeserializeRow converts bytes back to a Row struct
func DeserializeRow(src []byte, row *Row) {
	row.ID = int32(binary.LittleEndian.Uint32(src[0:4]))
	copy(row.Username[:], src[4:4+ColumnUsernameSize])
	copy(row.Email[:], src[4+ColumnUsernameSize:4+ColumnUsernameSize+ColumnEmailSize])
}

// GetMaxKeyFromPage returns the max key from a page (works for both leaf and internal)
func GetMaxKeyFromPage(page []byte) (uint32, error) {
	nodeType := GetNodeTypeFromPage(page)
	switch nodeType {
	case NodeTypeLeaf:
		node, err := DeserializeLeafNode(page)
		if err != nil {
			return 0, err
		}
		return node.GetMaxKey(), nil
	case NodeTypeInternal:
		node, err := DeserializeInternalNode(page)
		if err != nil {
			return 0, err
		}
		return node.GetMaxKey(), nil
	default:
		return 0, fmt.Errorf("unknown node type: %d", nodeType)
	}
}

// For backward compatibility with existing code, expose constants with old names
const (
	CommonHeaderSize = 6 // NodeType(1) + IsRoot(1) + Parent(4)
)

// WriteNodeToPage serializes a node (either LeafNode or InternalNode) to the provided writer
func WriteNodeToPage(w io.Writer, node interface{}) error {
	return binary.Write(w, binary.LittleEndian, node)
}
