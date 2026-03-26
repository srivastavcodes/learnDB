package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIO_MemoryStore(t *testing.T) {
	pages := []*btreeNode{
		{isLeaf: true},
		{isLeaf: true},
		{isLeaf: true},
	}
	var mem memoryStore
	for _, page := range pages {
		_ = mem.append(page)
	}
	for i, page := range pages {
		node, err := mem.fetch(uint64(i))
		require.NoError(t, err)
		require.Equal(t, node, page)
	}
}
