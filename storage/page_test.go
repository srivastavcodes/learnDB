package storage

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPage_CellOffsetByKey(t *testing.T) {
	bn := btreeNode{
		slots:  []uint16{0, 3, 1, 2},
		isLeaf: false,
	}
	bn.internalCells = make([]*internalCell, len(bn.slots))
	bn.internalCells = []*internalCell{
		{key: 1000},
		{key: 1003},
		{key: 1005},
		{key: 1001},
	}
	offset, ok := bn.cellOffsetByKey(1001)
	require.True(t, ok)
	require.Equal(t, 1, offset)

	offset, ok = bn.cellOffsetByKey(1004)
	require.False(t, ok)
	require.Equal(t, 3, offset)

	offset, ok = bn.cellOffsetByKey(1005)
	require.True(t, ok)
	require.Equal(t, 3, offset)
}

func TestPage_LeafNodeFull(t *testing.T) {
	bn := &btreeNode{isLeaf: true}
	for i := uint32(0); i < maxLeafNodeElems; i++ {
		bn.appendLeafCell(i, []byte("Hello"))
	}
	require.True(t, bn.isFull())

	bn = &btreeNode{isLeaf: true}
	for i := uint32(0); i < maxLeafNodeElems-1; i++ {
		bn.appendLeafCell(i, []byte("Hello"))
	}
	require.False(t, bn.isFull())
}

func TestPage_InternalNodeFull(t *testing.T) {
	bn := &btreeNode{isLeaf: false}
	for i := uint32(0); i < maxInternalNodeElems; i++ {
		bn.appendInternalCell(i, rand.Uint64())
	}
	require.True(t, bn.isFull())

	bn = &btreeNode{isLeaf: false}
	for i := uint32(0); i < maxInternalNodeElems-1; i++ {
		bn.appendInternalCell(i, rand.Uint64())
	}
	require.False(t, bn.isFull())
}

func TestPage_LeafNodeSplit(t *testing.T) {
	bn := btreeNode{isLeaf: true}
	for i := uint32(0); i < 5; i++ {
		bn.appendLeafCell(i, []byte(fmt.Sprintf("Hello%d", i)))
	}
	newpg := &btreeNode{isLeaf: true}
	key := bn.split(newpg)
	require.Equal(t, uint32(2), key)
	require.Equal(t, 3, len(newpg.slots))
	require.Equal(t, 3, len(newpg.leafCells))

	for i := uint32(0); i < 2; i++ {
		actual := bn.leafCells[bn.slots[i]]
		require.Equal(t, i, actual.key)
		require.Equal(t,
			uint32(len([]byte(fmt.Sprintf("Hello%d", i)))),
			actual.valueSize,
		)
		require.Equal(t,
			fmt.Appendf(make([]byte, 0), "Hello%d", i),
			actual.value,
		)
	}
	for i := uint32(0); i < 3; i++ {
		actual := newpg.leafCells[newpg.slots[i]]
		require.Equal(t, i+2, actual.key)
		require.Equal(t,
			uint32(len([]byte(fmt.Sprintf("Hello%d", i+2)))),
			actual.valueSize,
		)
		require.Equal(t,
			fmt.Appendf(make([]byte, 0), "Hello%d", i+2),
			actual.value,
		)
	}
}

func TestPage_InternalNodeSplit(t *testing.T) {
	bn := btreeNode{isLeaf: false}
	for i := uint32(0); i < 5; i++ {
		bn.appendInternalCell(i, uint64(uint32(100)+i))
	}
	newpg := &btreeNode{isLeaf: false}
	key := bn.split(newpg)
	require.Equal(t, uint32(2), key)
	require.Equal(t, 2, len(newpg.slots))
	require.Equal(t, 2, len(newpg.internalCells))

	for i := uint32(0); i < 2; i++ {
		actual := bn.internalCells[bn.slots[i]]
		require.Equal(t, i, actual.key)
		require.Equal(t, uint64(uint32(100)+i), actual.offset)
	}
	for i := uint32(0); i < 2; i++ {
		actual := newpg.internalCells[newpg.slots[i]]
		require.Equal(t, i+3, actual.key)
		require.Equal(t, uint64(uint32(100)+i+3), actual.offset)
	}
}
