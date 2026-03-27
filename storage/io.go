package storage

import "fmt"

type store interface {
	// append appends a node at the end of the store.
	append(node *btreeNode) error
	// update updates the node at the provided node's offset.
	update(node *btreeNode) error
	// fetch returns the node at the offset in the store.
	fetch(offset uint64) (*btreeNode, error)
	nextLSN() uint64
	incrLSN()
	// fsync persists the data in stable storage (on disk).
	fsync() error
	// incrLastKey is a thin wrapper around the lastKey field in the store.
	incrLastKey()
	// getLastKey returns the lastKey fields value from the store.
	getLastKey() uint32
}

type memoryStore struct {
	pages   []*btreeNode
	lastKey uint32
}

func (ms *memoryStore) getLastKey() uint32 {
	return ms.lastKey
}

func (ms *memoryStore) nextLSN() uint64 {
	return 0
}

func (ms *memoryStore) incrLSN() {
	return
}

func (ms *memoryStore) incrLastKey() {
	ms.lastKey++
}

func (ms *memoryStore) fsync() error {
	return nil
}

func (ms *memoryStore) append(node *btreeNode) error {
	node.offset = uint64(len(ms.pages))
	ms.pages = append(ms.pages, node)
	return nil
}

func (ms *memoryStore) update(node *btreeNode) error {
	index := node.offset
	ms.pages[index] = node
	return nil
}

func (ms *memoryStore) fetch(offset uint64) (*btreeNode, error) {
	if offset >= uint64(len(ms.pages)) {
		return nil, fmt.Errorf("page does not exist")
	}
	return ms.pages[offset], nil
}
