package storage

import (
	"fmt"
)

const (
	InternalNode = iota
	LeafNode
)

const (
	// maximum size of a serialized page in bytes.
	pageSize = 4096

	// space used to store internal node metadata.
	//
	// cellType(1) + fileOffset(8) + lastLogSequenceNumber(8) + rightOffset(8) +
	// cellCount(4) + freeSize(2).
	internalNodeHeaderSize = 1 + 8*3 + 4 + 2

	// space used to store leaf node metadata.
	//
	// cellType(1) + fileOffset(8) + lastLogSequenceNumber(8) + hasLeftSib(1) +
	// hasRightSib(1) + lSibFileOffset(8) + rSibFileOffset(8) + cellCount(4) +
	// freeSize(2).
	leafNodeHeaderSize = 1 + 8*2 + 1*2 + 8*2 + 4 + 2

	// size of the offset element array in bytes.
	offsetElemArraySize = 2

	// size of internal node key cell in bytes.
	//
	// key(4) + fileOffset(8)
	internalNodeCellSize = 4 + 8

	// todo: add a slotted page design for leaf cell.
	// maximum size of key/val cell's value.
	maxValueSize = 400

	// size of leaf node key/val cell.
	// key(4) + deleted(1) + valSize(4) + value(400)
	leafNodeCellSize = 4 + 1 + 4 + maxValueSize
)

const (
	// maximum number of non-leaf node elements.
	maxInternalNodeElems = (pageSize - internalNodeHeaderSize) /
		(offsetElemArraySize + internalNodeCellSize)

	// maximum number of leaf node elements.
	maxLeafNodeElems = (pageSize - leafNodeHeaderSize) /
		(offsetElemArraySize + leafNodeCellSize)
)

var ErrRowTooLarge = fmt.Errorf("row exceeds %d bytes", maxValueSize)

// internalCell is an entry in the internal node (non-leaf) of the B+Tree.
// It stores a key and a pointer to the child page that contains all the
// keys less than (or equal to) this key.
type internalCell struct {
	key    uint32
	offset uint64 // offset of the child page less than the key.
}

// leafCell holds the data entry in a leaf node, this is the actual row value.
type leafCell struct {
	// parent is a back-pointer to the page that owns this cell, which is needed for
	// update and delete operations that modify a cell after a scan returns it.
	// This does not get serialized.
	parent  *btreeNode
	key     uint32
	valSize uint32
	value   []byte

	// deleted is a tombstone marker for scans or point queries to make sure this
	// cell is skipped. The space is reclaimed during compaction.
	deleted bool
}

// btreeNode represents one page of the B+Tree. A single page is of 4096 bytes.
// A single btreeNode can be either an internal node or a leaf node.
type btreeNode struct {
	position uint64   // position of this node in the database file.
	slots    []uint16 // slot array in sorted order.
	freeSize uint16   // bytes of free space between header and cell data.
	dirty    bool     // whether the page has been modified since last in memory.
	lastLSN  uint64   // the last wal entry that modified this page.
	isLeaf   bool     // is a leaf page or not?
	// for internal node
	internalCells []*internalCell
	rightOffset   uint64 // offset of the rightmost child (not stored with a key).
	// for leaf node
	leafCells  []*leafCell
	hasLSib    bool   // true if it has a left sibling leaf.
	hasRSib    bool   // true if it has a right sibling leaf.
	lSibOffset uint64 // offset of the left sibling.
	rSibOffset uint64 // offset of the right sibling.
}

func (bn *btreeNode) markDirty(lsn uint64) {
	bn.lastLSN = lsn
	bn.dirty = true
}

// cellKey indexes directly into the leafCells or internalCells. The provided index
// must be an actual index and not a logical one.
// E.g.: _ = cellKey[n.slots[i]); returns the key.
func (bn *btreeNode) cellKey(offset uint16) uint32 {
	if bn.isLeaf {
		return bn.leafCells[offset].key
	}
	return bn.internalCells[offset].key
}

// appendLeafCell appends into the slot count the logical index of the cell being
// inserted, and append a pointer to a new leafCell into the node's leafCells.
func (bn *btreeNode) appendLeafCell(key uint32, value []byte) {
	bn.slots = append(bn.slots, uint16(len(bn.slots)))
	bn.leafCells = append(bn.leafCells, &leafCell{
		key: key, value: value,
		valSize: uint32(len(value)),
	})
}

// appendInternalCell appends into the slot count the logical index of the cell being
// inserted, and append a pointer to a new internalCell into the node's internalCells.
func (bn *btreeNode) appendInternalCell(key uint32, offset uint64) {
	bn.slots = append(bn.slots, uint16(len(bn.slots)))
	bn.internalCells = append(bn.internalCells, &internalCell{
		key: key, offset: offset,
	})
}

// insertInternalNode inserts into the internal node cells a new entry.
// E.g.: offset = 1, key = 15, position = pageD
// internalCells = [pageA: 10, pageB: 20, *pageC], len = 2 (cause the last pointer isn't an entry)
// bn.slots = [0, 1] => [0, 1, 1]. #line1
// bn.slots[1] = 2 => [0, 2, 1]. #line2
// internalCells => [pageA: 10, pageB: 20, pageD: 15, *pageC].
// But this internalNode structure is wrong, cause pageD should be left child of 20
// because of B+Tree's internal node properties and pageB should be left child of 15.
// So, we'll swap the page offsets.
func (bn *btreeNode) insertInternalNode(offset uint32, key uint32, position uint64) {
	bn.slots = append(bn.slots[:offset+1], bn.slots[:offset]...)
	bn.slots[offset] = uint16(len(bn.internalCells))
	bn.internalCells = append(bn.internalCells, &internalCell{
		key:    key,
		offset: position,
	})
	// internalCells = [pageA: 10, pageB: 20, pageD: 15, *pageC]
	// offset of the 1st index in slot [0, 2, 1] => 2 index in internalCells => pageD:15
	// rightOffset = pageD
	rightOffset := bn.internalCells[bn.slots[offset]].offset
	// offset of the 2nd index in slot [0, 2, 1] => 1 index in internalCells => pageB:20
	// leftOffset = pageB
	leftOffset := bn.internalCells[bn.slots[offset+1]].offset
	// becomes => pageB:15
	bn.internalCells[bn.slots[offset]].offset = leftOffset
	// becomes => pageD:20
	bn.internalCells[bn.slots[offset+1]].offset = rightOffset
}
