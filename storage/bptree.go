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

// BpTree is a B+ tree implementation that stores nodes in a backing store
// and maintains a reference to the root node's offset.
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

// insert adds a new record with the given value to the B+ tree, automatically assigning
// the next available key and LSN, returning the assigned key, LSN, and any error.
func (bp *BpTree) insert(value []byte) (uint32, uint64, error) {
	nextKey := bp.store.getLastKey() + 1
	nextLsn := bp.store.nextLSN()

	err := bp.insertKey(nextKey, nextLsn, value)

	bp.store.incrLastKey()
	bp.store.incrLSN()
	return nextKey, nextLsn, err
}

// insertKey inserts a key-value pair with the given LSN into the B+ tree, traversing
// from the root and delegating to leaf or internal node insertion based on node-type.
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

// insertLeaf inserts a key-value pair into a leaf node of the B+ tree or returns
// an error if the key already exists.
// When the leaf node becomes full after insertion, it splits the node and creates
// a new sibling, propagating the split key upward by updating or creating a parent
// node. All modified nodes are marked dirty with the provided LSN to track the
// transaction.
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
	newPage := btreeNode{isLeaf: true}

	err := bp.store.append(&newPage)
	if err != nil {
		return fmt.Errorf("append new page failed: %w", err)
	}
	// first key of the newPage
	newKey := curr.splitAppendTo(&newPage)

	// pointer re-shuffling to mark sibling nodes
	oldRSibOffset := curr.rSibOffset

	curr.hasRSib = true
	curr.rSibOffset = newPage.offset

	newPage.hasLSib = true
	newPage.lSibOffset = curr.offset

	if parent == nil {
		var parentNode btreeNode

		err = bp.store.append(&parentNode)
		if err != nil {
			return fmt.Errorf("appending parent page failed: %w", err)
		}
		bp.setRoot(&parentNode)

		parentNode.rightOffset = newPage.offset
		parentNode.appendInternalCell(newKey, curr.offset)

		parent = &parentNode
	} else if newKey > parent.rightMostKey() {
		parent.appendInternalCell(newKey, parent.rightOffset)
		parent.rightOffset = newPage.offset
	} else {
		newPage.rSibOffset = oldRSibOffset
		newPage.hasRSib = true

		// we are going to insert a new internal node, so it shouldn't be
		// present on the parent node already.
		index, present = parent.cellOffsetByKey(newKey)
		if present {
			return fmt.Errorf("%w for key=%d", ErrKeyAlreadyExists, key)
		}
		parent.insertInternalCell(uint32(index), newKey, newPage.offset)

		// update previous right sibling's left pointer
		rightSib, err := bp.store.fetch(oldRSibOffset)
		if err != nil {
			return fmt.Errorf(
				"couldn't fetch right sibling %d: %w", oldRSibOffset, err,
			)
		}
		rightSib.lSibOffset = newPage.offset
		rightSib.markDirty(lsn)
	}
	// mark the pages dirty for the given log sequence number to denote part
	// of the same transaction.
	newPage.markDirty(lsn)
	curr.markDirty(lsn)
	parent.markDirty(lsn)
	return nil
}

// insertInternal recursively inserts a key-value pair into an internal node of the B+ tree,
// traversing down to the appropriate child node based on the key's position.
// When the internal node becomes full after insertion, it splits the node, promotes the
// middle key to the parent, and creates a new sibling node to balance the tree.
// All modified nodes are marked dirty with the provided LSN to track the transaction.
func (bp *BpTree) insertInternal(parent, curr *btreeNode, key uint32, lsn uint64, val []byte) error {
	index, present := curr.cellOffsetByKey(key)
	if present {
		return fmt.Errorf("%w for key=%d", ErrKeyAlreadyExists, key)
	}
	offset := uint64(0)
	// assigning the child node's offset depending on where the key will be inserted.
	if index == len(curr.slots) {
		offset = curr.rightOffset
	} else {
		offset = curr.internalCells[curr.slots[index]].offset
	}
	childPage, err := bp.store.fetch(offset)
	if err != nil {
		return fmt.Errorf("couldn't fetch node at offset %d: %w", offset, err)
	}
	if childPage.isLeaf {
		if err = bp.insertLeaf(curr, childPage, key, lsn, val); err != nil {
			return err
		}
	} else {
		if err = bp.insertInternal(curr, childPage, key, lsn, val); err != nil {
			return err
		}
	}
	if !curr.isFull() {
		return nil
	}
	var newPage btreeNode

	err = bp.store.append(&newPage)
	if err != nil {
		return fmt.Errorf("appending new page failed: %w", err)
	}
	// newKey is the middle key of the curr node before splitting; now that it has been
	// split, there are two cells before and after the mid-key.
	// the mid-key will be appended to the parent, and it will store the previously right
	// most offset of the parent.
	// and now the parent will store the newPage's offset as the right-most offset since
	// a new page has been added.
	// following the order in which the recursion is happening, if you follow through
	// after the new key has been added to the parent, there are two cases, node full or
	// not full.
	// if full, then, after this stack returns, the now-parent node will be checked if it
	// is full or not, if it is, the same thing happens again until some space is found or
	// eventually a new root is created.

	// todo: drawing it out might help.
	newKey := curr.splitAppendTo(&newPage)

	if parent == nil {
		var parentNode btreeNode

		err = bp.store.append(&parentNode)
		if err != nil {
			return fmt.Errorf("appending parent page failed: %w", err)
		}
		bp.setRoot(&parentNode)

		parentNode.rightOffset = newPage.offset
		parentNode.appendInternalCell(newKey, curr.offset)

		parent = &parentNode
	} else {
		parent.appendInternalCell(newKey, parent.rightOffset)
		parent.rightOffset = newPage.offset
	}
	newPage.markDirty(lsn)
	curr.markDirty(lsn)
	parent.markDirty(lsn)
	return nil
}
