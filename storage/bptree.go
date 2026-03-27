package storage

import (
	"errors"
	"fmt"
)

var ErrKeyAlreadyExists = errors.New("record already exists")

// ScanAction signals whether the scan iterator can continue after processing
// an iterator callback.
type ScanAction bool

var (
	CanScan  = ScanAction(true)
	CantScan = ScanAction(false)
)

type BpTree struct {
	store      store
	rootOffset uint64
}

// fetches the root node and returns it or an error if any.
func (bp *BpTree) root() (*btreeNode, error) {
	return bp.store.fetch(bp.rootOffset)
}

// sets the provided node's offset as the root offset.
func (bp *BpTree) setRoot(node *btreeNode) {
	bp.rootOffset = node.offset
}

func (bp *BpTree) insert(value []byte) (uint32, uint64, error) {
	nextKey := bp.store.getLastKey() + 1
	nextLsn := bp.store.nextLSN()

	err := bp.insertKey(nextKey, nextLsn, value)

	bp.store.incrLastKey()
	bp.store.incrLSN()
	return nextKey, nextLsn, err
}

func (bp *BpTree) insertKey(key uint32, lsn uint64, value []byte) error {
	root, err := bp.root()
	if err != nil {
		return fmt.Errorf("failed to fetch the root node: %w", err)
	}
	if root.isLeaf {
		return bp.insertLeaf(nil, root, key, lsn, value)
	}
	return bp.insertInternal(nil, root, key, lsn, value)
}

func (bp *BpTree) insertLeaf(parent, curr *btreeNode, key uint32, lsn uint64, val []byte) error {
	index, present := curr.cellOffsetByKey(key)
	if present { // not an update operation so return error.
		return fmt.Errorf("%w for key=%d", ErrKeyAlreadyExists, key)
	}
	if index != len(curr.slots) {
		if err := curr.insertLeafCell(uint32(index), key, val); err != nil {
			return fmt.Errorf("insertLeaf failed: %w", err)
		}
	} else {
		curr.appendLeafCell(key, val)
	}
	curr.markDirty(lsn)

	if !curr.isFull() {
		return nil
	}
	newpg := btreeNode{isLeaf: true}

	err := bp.store.append(&newpg)
	if err != nil {
		return fmt.Errorf("append new page failed: %w", err)
	}
	// first key of the newpg
	newKey := curr.split(&newpg)

	// pointer re-shuffling to mark sibling nodes
	oldRSibOffset := curr.rSibOffset

	curr.hasRSib = true
	curr.rSibOffset = newpg.offset

	newpg.hasLSib = true
	newpg.lSibOffset = curr.offset

	if parent == nil {
		var parentNode btreeNode

		err = bp.store.append(&parentNode)
		if err != nil {
			return fmt.Errorf("appending parent page failed: %w", err)
		}
		bp.setRoot(&parentNode)

		parentNode.rightOffset = newpg.offset
		parentNode.appendInternalCell(newKey, curr.offset)

		parent = &parentNode
	} else if newKey > parent.rightMostKey() {
		parent.appendInternalCell(newKey, parent.rightOffset)
		parent.rightOffset = newpg.offset
	} else {
		newpg.rSibOffset = oldRSibOffset
		newpg.hasRSib = true

		// we are going to insert a new internal node, so it shouldn't be
		// present on the parent node already.
		index, present = parent.cellOffsetByKey(newKey)
		if present {
			return fmt.Errorf("%w for key=%d", ErrKeyAlreadyExists, key)
		}
		parent.insertInternalCell(uint32(index), newKey, newpg.offset)
		// update previous right sibling's left pointer
		rightSib, err := bp.store.fetch(oldRSibOffset)
		if err != nil {
			return fmt.Errorf("couldn't fetch right sibling %d: %w",
				oldRSibOffset, err,
			)
		}
		rightSib.lSibOffset = newpg.offset
		rightSib.markDirty(lsn)
	}
	// mark the pages dirty for the given log sequence number to denote part
	// of the same transaction.
	newpg.markDirty(lsn)
	curr.markDirty(lsn)
	parent.markDirty(lsn)
	return nil
}

func (bp *BpTree) insertInternal(parent, curr *btreeNode, key uint32, lsn uint64, val []byte) error {
	return nil
}
