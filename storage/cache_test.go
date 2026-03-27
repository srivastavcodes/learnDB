package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLRUGet_Found(t *testing.T) {
	lru := NewLru(3)

	lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
	lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
	lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})

	key := "A"
	expect := &btreeNode{isLeaf: true, offset: 1}

	val := lru.entry(key)
	require.NotNil(t, val)
	require.Equal(t, expect.offset, val.offset)
}

func TestLRUGet_NotFound(t *testing.T) {
	lru := NewLru(3)

	lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
	lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
	lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})

	key := "D"
	val := lru.entry(key)
	require.Nil(t, val)
}

func TestLRUState(t *testing.T) {
	tests := []struct {
		name        string
		expectCount int
		maxEntries  int
		setState    func(*LruCache)
		expectFront any
		expectBack  any
	}{
		{
			name:        "adding new element to full cache should evict oldest element",
			maxEntries:  5,
			expectCount: 5,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				lru.setEntry("D", &btreeNode{isLeaf: true, offset: 4})
				lru.setEntry("E", &btreeNode{isLeaf: true, offset: 5})
				// this should evict element A
				lru.setEntry("F", &btreeNode{isLeaf: true, offset: 6})
			},
			expectFront: "F",
			expectBack:  "B",
		},
		{
			name:        "updating existing element should move it to the head of the list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				// moved to beginning of list
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
			},
			expectFront: "A",
			expectBack:  "B",
		},
		{
			name:        "retrieving existing element should move it to the head of the list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				lru.entry("A") // moved to beginning of list
			},
			expectFront: "A",
			expectBack:  "B",
		},
		{
			name:        "retrieving non-existing element should not affect list",
			expectCount: 3,
			maxEntries:  5,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				lru.entry("X") // this element should not exist
			},
			expectFront: "C",
			expectBack:  "A",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lru := NewLru(tt.maxEntries)
			tt.setState(lru)
			require.Equal(t, tt.expectCount, len(lru.cache))
			require.Equal(t, tt.expectFront, lru.list.Front().Value.(*cacheEntry).key)
			require.Equal(t, tt.expectBack, lru.list.Back().Value.(*cacheEntry).key)
		})
	}
}

func TestLRUEviction(t *testing.T) {
	tests := []struct {
		name        string
		expectCount int
		maxEntries  int
		setState    func(*LruCache)
		expectKeys  []string
	}{
		{
			name:        "adding new element to full cache should evict oldest element",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				// this should evict element A
				lru.setEntry("D", &btreeNode{isLeaf: true, offset: 4})
			},
			expectKeys: []string{"D", "C", "B"},
		},
		{
			name:        "adding new element to full cache should evict 2nd-oldest element",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1, isDirty: true})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				// this should evict element B
				lru.setEntry("D", &btreeNode{isLeaf: true, offset: 4})
			},
			expectKeys: []string{"D", "C", "A"},
		},
		{
			name:        "adding new element to full cache should evict 3rd-oldest element",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1, isDirty: true})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2, isDirty: true})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3})
				// this should evict element C
				lru.setEntry("D", &btreeNode{isLeaf: true, offset: 4})
			},
			expectKeys: []string{"D", "B", "A"},
		},
		{
			name:        "adding new element to full cache should fail",
			maxEntries:  3,
			expectCount: 3,
			setState: func(lru *LruCache) {
				lru.setEntry("A", &btreeNode{isLeaf: true, offset: 1, isDirty: true})
				lru.setEntry("B", &btreeNode{isLeaf: true, offset: 2, isDirty: true})
				lru.setEntry("C", &btreeNode{isLeaf: true, offset: 3, isDirty: true})
				res := lru.setEntry("D", &btreeNode{isLeaf: true, offset: 4})
				require.False(t, res,
					"expected setEntry to fail because of cache full, but it succeeded",
				)
			},
			expectKeys: []string{"C", "B", "A"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lru := NewLru(tt.maxEntries)
			tt.setState(lru)
			require.Equal(t, tt.expectCount, len(lru.cache))

			cur := lru.list.Front()

			for _, expectK := range tt.expectKeys {
				actualK := cur.Value.(*cacheEntry).key
				require.Equal(t, expectK, actualK)
				cur = cur.Next()
			}
		})
	}
}
