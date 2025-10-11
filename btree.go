package main

import "unsafe"

// TODO: Use proper go structs with

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
	LeafNodeHeaderSize     = CommonHeaderSize + LeafNodeNumCellsSize
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

// Leaf Node Page Layout
//
//   0                   1                   2                   3
//   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |   NodeType    |    IsRoot     |       ParentPointer           |  bytes 0..5
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                    LeafNodeNumCells (uint32)                  |  bytes 6..9
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Cell[0]                                |  bytes 10..(10+CellSize-1)
//  |  Key (u32)  |                Value (rowSize bytes)            |
//  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//  |                        Cell[1]                                |  bytes (10+1*CellSize)..(10+2*CellSize-1)
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
	*leafNodeNumCells(node) = 0
}
