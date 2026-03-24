package storage

import (
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
