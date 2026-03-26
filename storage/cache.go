package storage

import "container/list"

type LruCache struct {
	cache map[any]*list.Element
	size  int
	list  *list.List
}

type cacheEntry struct {
	key   any
	value *btreeNode
}

func NewLru(size int) *LruCache {
	return &LruCache{
		cache: make(map[any]*list.Element),
		list:  list.New(), size: size,
	}
}

// set sets the key/value pair into the cache if len(cache) != size.
// If maximum size has been reached, it attempts to evict a non-dirty
// page and add the new page, otherwise returns false if no non-dirty
// pages were found.
//
// If the key existed already, the old value is replaced with the new
// value, and the key becomes the most recently used.
func (lru *LruCache) setEntry(key any, val *btreeNode) bool {
	if entry, present := lru.cache[key]; present {
		entry.Value.(*cacheEntry).value = val
		lru.list.MoveToFront(entry)
		return present
	}
	if len(lru.cache) == lru.size {
		curr := lru.list.Back()
	checkLoop:
		for {
			switch {
			case curr == nil:
				return false
			// a page that is not dirty and thus can be evicted.
			case !curr.Value.(*cacheEntry).value.isDirty:
				break checkLoop
			default:
				curr = curr.Prev()
			}
		}
		lru.list.Remove(curr)
		delete(lru.cache, curr.Value.(*cacheEntry).key)
	}
	// add the element to the front of the list.
	entry := lru.list.PushFront(&cacheEntry{key, val})
	lru.cache[key] = entry
	return true
}

// entry returns the node cell if present and pushes the element at the front
// of the list; nil otherwise.
func (lru *LruCache) entry(key any) *btreeNode {
	if entry, present := lru.cache[key]; present {
		lru.list.MoveToFront(entry)
		return entry.Value.(*cacheEntry).value
	}
	return nil
}
