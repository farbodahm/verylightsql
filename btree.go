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
	InternalNodeMaxKeys   = 3
)

// Internal node split constants
// When splitting an internal node with InternalNodeMaxKeys keys, we need to
// redistribute (InternalNodeMaxKeys + 1) keys (including the new one being inserted).
// The middle key goes up to the parent, and the rest are split between left and right.
const (
	InternalNodeRightSplitCount = (InternalNodeMaxKeys + 1) / 2                               // Keys that go to right node
	InternalNodeLeftSplitCount  = (InternalNodeMaxKeys + 1) - InternalNodeRightSplitCount - 1 // Keys that stay in left node (-1 for key promoted to parent)
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
	offset := InternalNodeHeaderSize + int(cellNum)*InternalNodeCellSize + InternalNodeChildSize
	return (*uint32)(unsafe.Pointer(&node[offset]))
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

// internalNodeFindChild returns the index of the child pointer which should contain the given key
func internalNodeFindChild(node []byte, key uint32) uint32 {
	// Binary search
	numKeys := *internalNodeNumKeys(node)
	i, j := uint32(0), numKeys
	for i != j {
		mid := (i + j) / 2
		midKey := *internalNodeKey(node, mid)
		if key < midKey {
			j = mid
		} else {
			i = mid + 1
		}
	}
	return i
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

func nodeParent(node []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&node[ParentPointerOffset]))
}

func updateInternalNodeKey(node []byte, oldKey uint32, newKey uint32) {
	oldChildIndex := internalNodeFindChild(node, oldKey)
	*internalNodeKey(node, oldChildIndex) = newKey
}

// internalNodeFindChildByPage returns the index of the child with the given page number.
// Returns numKeys if the child is the right child.
// Panics if the child is not found.
func internalNodeFindChildByPage(node []byte, childPageNum uint32) uint32 {
	numKeys := *internalNodeNumKeys(node)
	for i := uint32(0); i < numKeys; i++ {
		if *internalNodeChild(node, i) == childPageNum {
			return i
		}
	}
	if *internalNodeRightChild(node) == childPageNum {
		return numKeys
	}
	panic("child not found in parent")
}

// internalNodeChildPtr returns a pointer to the child page number at the given cell index.
// Unlike internalNodeChild which handles the right child case specially,
// this returns the raw pointer to the child field in the cell.
func internalNodeChildPtr(node []byte, cellNum uint32) *uint32 {
	cell := internalNodeCell(node, cellNum)
	return (*uint32)(unsafe.Pointer(&cell[0]))
}
