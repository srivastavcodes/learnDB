package storage

import (
	"fmt"
)

const (
	InternalCell byte = iota
	LeafCell
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
	// key(4) + deleted(1) + valueSize(4) + value(400)
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
	parent    *btreeNode
	key       uint32
	valueSize uint32
	value     []byte

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
		valueSize: uint32(len(value)),
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

// insertInternalNode inserts into the internal node cells a new entry at the
// provided index. It panics if the provided index is equal to the length of
// the slots array - use appendInternalCell instead.
// E.g.: offset = 1, key = 15, position = pageD
// internalCells = [pageA: 10, pageB: 20, *pageC], len = 2 (cause the last pointer isn't an entry)
// bn.slots = [0, 1] => [0, 1, 1]. #line1
// bn.slots[1] = 2 => [0, 2, 1]. #line2
// internalCells => [pageA: 10, pageB: 20, pageD: 15, *pageC].
// But this internalNode structure is wrong, cause pageD should be left child of 20
// because of B+Tree's internal node properties and pageB should be left child of 15.
// So, we'll swap the page offsets.
func (bn *btreeNode) insertInternalNode(index uint32, key uint32, offset uint64) {
	bn.slots = append(bn.slots[:index+1], bn.slots[:index]...)
	bn.slots[index] = uint16(len(bn.internalCells))
	bn.internalCells = append(bn.internalCells, &internalCell{
		key: key, offset: offset,
	})
	// internalCells = [pageA: 10, pageB: 20, pageD: 15, *pageC]
	// offset of the 1st index in slot [0, 2, 1] => 2 index in internalCells => pageD:15
	// rightOffset = pageD
	rightOffset := bn.internalCells[bn.slots[index]].offset
	// offset of the 2nd index in slot [0, 2, 1] => 1 index in internalCells => pageB:20
	// leftOffset = pageB
	leftOffset := bn.internalCells[bn.slots[index+1]].offset
	// becomes => pageB:15
	bn.internalCells[bn.slots[index]].offset = leftOffset
	// becomes => pageD:20
	bn.internalCells[bn.slots[index+1]].offset = rightOffset
}

// insertLeafCell inserts a new leaf cell with the provided key/val pair at the
// given index. It panics if the provided index is equal to the length of the
// slots array - use appendLeafCell instead.
func (bn *btreeNode) insertLeafCell(index, key uint32, value []byte) error {
	if len(value) > maxValueSize {
		return ErrRowTooLarge
	}
	bn.slots = append(bn.slots[:index+1], bn.slots[index:]...)
	bn.slots[index] = uint16(len(bn.slots))
	bn.leafCells = append(bn.leafCells, &leafCell{
		key: key, value: value,
		valueSize: uint32(len(value)),
	})
	return nil
}

func (bn *btreeNode) updateCell(key uint32, value []byte) error {
	if len(value) > maxValueSize {
		return ErrRowTooLarge
	}
	offset, ok := bn.cellOffsetByKey(key)
	if !ok {
		return fmt.Errorf("key record does not exist: %d", key)
	}
	bn.leafCells[bn.slots[offset]].value = value
	bn.leafCells[bn.slots[offset]].valueSize = uint32(len(value))
	return nil
}

// cellOffsetByKey searches using b.s. for a cell by key. If ok is true, offset
// is the index of key in the cell slice, if ok is false, offset is the key's
// insertion point (index of the first element larger than the key).
func (bn *btreeNode) cellOffsetByKey(key uint32) (offset int, ok bool) {
	low, high := 0, len(bn.slots)-1
	for low <= high {
		mid := low + (high-low)/2
		curr := bn.cellKey(bn.slots[mid])
		switch {
		case curr < key:
			low = mid + 1
		case curr > key:
			high = mid - 1
		default:
			return mid, true
		}
	}
	return low, false
}

// isFull returns if the current node has reached its capacity depending on which
// type of page it is.
func (bn *btreeNode) isFull() bool {
	if bn.isLeaf {
		return len(bn.slots) >= maxLeafNodeElems
	}
	return len(bn.slots) >= maxInternalNodeElems
}

// split splits the current cell into two halves and appends the second half into
// the provided newpg. If the current node is a leaf node, then the second half
// will contain elements from the middle element of the previous cell; if it was
// an internal cell, then the newpg contains elements from mid+1 and the middle key
// is returned to be pushed to the parent.
func (bn *btreeNode) split(newpg *btreeNode) (key uint32) {
	if bn.isLeaf {
		mid := len(bn.slots) / 2
		for i := mid; i < len(bn.slots); i++ {
			cell := bn.leafCells[bn.slots[i]]
			newpg.appendLeafCell(cell.key, cell.value)
		}
		bn.slots = bn.slots[0:mid]
		return newpg.leafCells[newpg.slots[0]].key
	}
	mid := len(bn.slots) / 2
	for i := mid + 1; i < len(bn.slots); i++ {
		cell := bn.internalCells[bn.slots[i]]
		newpg.appendInternalCell(cell.key, cell.offset)
	}
	newpg.rightOffset = bn.rightOffset

	// mid-key gets saved to be returned.
	key = bn.internalCells[mid].key
	// the middle key's offset now becomes the right offset.
	bn.rightOffset = bn.internalCells[mid].offset
	// mid-key removed from this cell's slots.
	bn.slots = bn.slots[0:mid]
	return
}
