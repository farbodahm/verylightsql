package main

import (
	"fmt"
	"unsafe"
)

// TODO: Use proper go structs with serialization instead of byte arrays and unsafe

// NodeType represents the type of a B-tree node.
// It can be either an internal node or a leaf node.
type NodeType uint8

const (
	NodeTypeInternal NodeType = iota
	NodeTypeLeaf
)

// Common node header Layout used by both internal and leaf nodes.
const (
	NodeTypeSize        = int(unsafe.Sizeof(NodeType(0)))
	NodeTypeOffset      = 0
	IsRootSize          = int(unsafe.Sizeof(uint8(0)))
	IsRootOffset        = NodeTypeOffset + NodeTypeSize
	ParentPointerSize   = int(unsafe.Sizeof(uint32(0)))
	ParentPointerOffset = IsRootOffset + IsRootSize
	CommonHeaderSize    = NodeTypeSize + IsRootSize + ParentPointerSize
)

// Leaf node header Layout.
const (
	LeafNodeNumCellsSize   = int(unsafe.Sizeof(uint32(0)))
	LeafNodeNumCellsOffset = CommonHeaderSize
	LeafNodeNextLeafSize   = int(unsafe.Sizeof(uint32(0)))
	LeafNodeNextLeafOffset = LeafNodeNumCellsOffset + LeafNodeNumCellsSize
	LeafNodeHeaderSize     = CommonHeaderSize + LeafNodeNumCellsSize + LeafNodeNextLeafSize
)

// Leaf node body Layout.
const (
	LeafNodeKeySize       = int(unsafe.Sizeof(uint32(0)))
	LeafNodeKeyOffset     = 0
	LeafNodeValueSize     = rowSize
	LeafNodeValueOffset   = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize      = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells = pageSize - LeafNodeHeaderSize
	LeafNodeMaxCells      = LeafNodeSpaceForCells / LeafNodeCellSize
)

const (
	LeafNodeRightSplitCount = (LeafNodeMaxCells + 1) / 2 // +1 is for the new cell we are adding
	LeafNodeLeftSplitCount  = (LeafNodeMaxCells + 1) - LeafNodeRightSplitCount
)

// Leaf Node Page Layout
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |   NodeType    |    IsRoot     |       ParentPointer           |  bytes 0..5
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                    LeafNodeNumCells (uint32)                  |  bytes 6..9
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                    LeafNodeNextLeaf (uint32)                  |  bytes 10..13
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Cell[0]                                |  bytes 14..(14+CellSize-1)
//  |  Key (u32)  |                Value (rowSize bytes)            |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Cell[1]                                |  bytes (14+1*CellSize)..(14+2*CellSize-1)
//  |  Key (u32)  |                Value (rowSize bytes)            |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                              ...                              |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Cell[i]                                |  i in [0..NumCells-1]
//  |  Key @ (Hdr+i*CellSize) .. (Hdr+i*CellSize+3)                 |
//  |  Val @ (Hdr+i*CellSize+4) .. (Hdr+(i+1)*CellSize-1)           |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                          Free Space                           |  bytes (Hdr+NumCells*CellSize)..(pageSize-1)
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

// Internal node header layout
const (
	InternalNodeNumKeysSize      = int(unsafe.Sizeof(uint32(0)))
	InternalNodeNumKeysOffset    = CommonHeaderSize
	InternalNodeRightChildSize   = int(unsafe.Sizeof(uint32(0)))
	InternalNodeRightChildOffset = InternalNodeNumKeysOffset + InternalNodeNumKeysSize
	InternalNodeHeaderSize       = CommonHeaderSize + InternalNodeNumKeysSize + InternalNodeRightChildSize
)

// Internal node body layout
const (
	InternalNodeKeySize   = int(unsafe.Sizeof(uint32(0)))
	InternalNodeChildSize = int(unsafe.Sizeof(uint32(0)))
	InternalNodeCellSize  = InternalNodeKeySize + InternalNodeChildSize
)

// Internal Node Layout
//
// Internal nodes store keys and child pointers that guide tree traversal.
// Each cell in the body consists of a (child_pointer, key) pair.
// The header also contains an additional "right child" pointer,
// making a total of (num_keys + 1) child pointers per internal node.
//
// Internal Node Page Layout:
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |   NodeType    |    IsRoot     |       ParentPointer           |  bytes 0..5
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                    InternalNodeNumKeys (uint32)               |  bytes 6..9
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                    InternalNodeRightChild (uint32)            |  bytes 10..13
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                           Cell[0]                             |  bytes Hdr..(Hdr+CellSize-1)
//  |  Child (u32) |                    Key (u32)                   |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                           Cell[1]                             |  bytes (Hdr+1*CellSize)..(Hdr+2*CellSize-1)
//  |  Child (u32) |                    Key (u32)                   |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                              ...                              |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                           Cell[i]                             |  i in [0..NumKeys-1]
//  |  Ch @ (Hdr+i*CellSize) .. (Hdr+i*CellSize+3)                  |
//  |  Ky @ (Hdr+i*CellSize+4) .. (Hdr+(i+1)*CellSize-1)            |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                          Free Space                           |  bytes (Hdr+NumKeys*CellSize)..(pageSize-1)
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

// Leaf node helper functions

func leafNodeNumCells(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LeafNodeNumCellsOffset]))
}
func leafNodeCell(node []byte, cellNum uint32) []byte {
	start := LeafNodeHeaderSize + int(cellNum)*LeafNodeCellSize
	end := start + LeafNodeCellSize
	return node[start:end]
}
func leafNodeKey(node []byte, cellNum uint32) *uint32 {
	cell := leafNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cell[LeafNodeKeyOffset]))
}
func leafNodeValue(node []byte, cellNum uint32) []byte {
	cell := leafNodeCell(node, cellNum)
	return cell[LeafNodeValueOffset : LeafNodeValueOffset+LeafNodeValueSize]
}
func initializeLeafNode(node []byte) {
	*nodeType(node) = NodeTypeLeaf
	setNodeRoot(node, false)
	*leafNodeNumCells(node) = 0
	*leafNodeNextLeaf(node) = 0 // 0 means no sibling
}

func nodeType(node []byte) *NodeType {
	return (*NodeType)(unsafe.Pointer(&node[NodeTypeOffset]))
}

func isNodeRoot(node []byte) bool {
	return node[IsRootOffset] != 0
}

func setNodeRoot(node []byte, isRoot bool) {
	if isRoot {
		node[IsRootOffset] = 1
	} else {
		node[IsRootOffset] = 0
	}
}

func leafNodeNextLeaf(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[LeafNodeNextLeafOffset]))
}

// Internal node helper functions

func internalNodeNumKeys(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[InternalNodeNumKeysOffset]))
}

func internalNodeRightChild(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[InternalNodeRightChildOffset]))
}

func internalNodeCell(node []byte, cellNum uint32) []byte {
	start := InternalNodeHeaderSize + int(cellNum)*InternalNodeCellSize
	end := start + InternalNodeCellSize
	return node[start:end]
}

func internalNodeKey(node []byte, cellNum uint32) *uint32 {
	cell := internalNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cell[InternalNodeChildSize]))
}

func internalNodeChild(node []byte, cellNum uint32) *uint32 {
	numKeys := *internalNodeNumKeys(node)
	if cellNum > numKeys {
		panic(fmt.Sprintf("Tried to access child_num %d > num_keys %d", cellNum, numKeys))
	} else if cellNum == numKeys {
		return internalNodeRightChild(node)
	} else {
		cell := internalNodeCell(node, cellNum)
		return (*uint32)(unsafe.Pointer(&cell[0]))
	}
}

func getNodeMaxKey(node []byte) uint32 {
	nType := *nodeType(node)
	if nType == NodeTypeLeaf {
		numCells := *leafNodeNumCells(node)
		return *leafNodeKey(node, numCells-1)
	} else if nType == NodeTypeInternal {
		numKeys := *internalNodeNumKeys(node)
		return *internalNodeKey(node, numKeys-1)
	} else {
		panic("Unknown node type")
	}
}

func initializeInternalNode(node []byte) {
	*nodeType(node) = NodeTypeInternal
	setNodeRoot(node, false)
	*internalNodeNumKeys(node) = 0
}
