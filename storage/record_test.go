package storage

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecord_EncodeDecodeInternalCell(t *testing.T) {
	want := btreeNode{
		position: 10,
		slots:    []uint16{2, 1, 0, 3},
		freeSize: 4009,
		lastLSN:  1234,
		internalCells: []*internalCell{
			{123, 3},
			{12, 8},
			{1, 6},
			{1234, 2},
		},
		rightOffset: 1,
	}
	buf, err := want.encodeInternalCell()
	require.NoError(t, err)
	require.Equal(t, pageSize, buf.Len())

	var got btreeNode
	err = got.decodeInternalCell(buf)
	require.NoError(t, err)

	require.Truef(t, reflect.DeepEqual(want, got), "want=%+v. \ngot=%+v", want, got)
}

func TestRecord_EncodeDecodeLeafCell(t *testing.T) {
	want := btreeNode{
		isLeaf:   true,
		position: 10,
		freeSize: 3926,
		slots:    []uint16{0, 1, 2, 3},
		lastLSN:  1234,
		leafCells: []*leafCell{
			{
				key: 1, value: []byte("first line in leaf"),
				valueSize: uint32(len("first line in leaf")),
			},
			{
				key: 2, value: []byte("this is my database"),
				valueSize: uint32(len("this is my database")),
			},
			{
				key: 3, value: []byte("i will write another one"),
				valueSize: uint32(len("i will write another one")),
			},
			{
				key: 4, value: []byte("boom badam # dhoom dhoom"),
				valueSize: uint32(len("boom badam # dhoom dhoom")),
			},
		},
	}
	buf, err := want.encodeLeafCell()
	require.NoError(t, err)
	require.Equal(t, pageSize, buf.Len())

	var got = btreeNode{isLeaf: true}
	err = got.decodeLeafCell(buf)
	require.NoError(t, err)

	require.Truef(t, reflect.DeepEqual(want, got), "\nwant=%+v. \ngot=%+v", want, got)
	fmt.Printf("\nwant=%+v. \ngot=%+v", want, got)
	for i := 0; i < len(want.leafCells); i++ {
		fmt.Printf("\nwant=%q. \ngot=%q", want.leafCells[i].value, got.leafCells[i].value)
	}
}
