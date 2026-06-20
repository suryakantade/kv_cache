package store

import (
	"container/list"
	"sync"
)

type lruEntry struct {
	key string
}

type lruList struct {
	mu      sync.Mutex
	l       *list.List
	entries map[string]*list.Element
	maxKeys int
}

func newLRUList(maxKeys int) *lruList {
	return &lruList{
		l:       list.New(),
		entries: make(map[string]*list.Element),
		maxKeys: maxKeys,
	}
}

// touch marks key as most recently used. Must be called with store write lock held.
func (r *lruList) touch(key string) {
	if r.maxKeys == 0 {
		return
	}
	if el, ok := r.entries[key]; ok {
		r.l.MoveToFront(el)
	} else {
		el = r.l.PushFront(&lruEntry{key: key})
		r.entries[key] = el
	}
}

// remove removes key from the LRU list.
func (r *lruList) remove(key string) {
	if r.maxKeys == 0 {
		return
	}
	if el, ok := r.entries[key]; ok {
		r.l.Remove(el)
		delete(r.entries, key)
	}
}

// evict returns the least-recently-used key, or "" if the list is empty.
func (r *lruList) evict() string {
	if r.maxKeys == 0 {
		return ""
	}
	back := r.l.Back()
	if back == nil {
		return ""
	}
	entry := back.Value.(*lruEntry)
	r.l.Remove(back)
	delete(r.entries, entry.key)
	return entry.key
}

// needsEviction returns true when the LRU is at capacity.
func (r *lruList) needsEviction(currentSize int) bool {
	return r.maxKeys > 0 && currentSize >= r.maxKeys
}
