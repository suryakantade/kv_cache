package store

import (
	"context"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time // zero means no expiry
}

func (e *entry) expired() bool {
	return !e.expiresAt.IsZero() && time.Now().After(e.expiresAt)
}

// Store is a thread-safe in-memory key-value store with LRU eviction and TTL.
type Store struct {
	mu     sync.RWMutex
	data   map[string]*entry
	lru    *lruList
	cancel context.CancelFunc
}

// New creates a new Store. maxKeys=0 means unlimited.
func New(maxKeys int) *Store {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Store{
		data:   make(map[string]*entry),
		lru:    newLRUList(maxKeys),
		cancel: cancel,
	}
	go s.startExpirer(ctx)
	return s
}

// Close stops the background expirer.
func (s *Store) Close() {
	s.cancel()
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// get retrieves a live (non-expired) entry. Caller must hold at least read lock.
func (s *Store) get(key string) (*entry, bool) {
	e, ok := s.data[key]
	if !ok {
		return nil, false
	}
	if e.expired() {
		return nil, false
	}
	return e, true
}

// set writes an entry, evicting the LRU key if needed. Caller must hold write lock.
func (s *Store) set(key, value string, ttl time.Duration) {
	if _, exists := s.data[key]; !exists && s.lru.needsEviction(len(s.data)) {
		victim := s.lru.evict()
		if victim != "" {
			delete(s.data, victim)
		}
	}
	e := &entry{value: value}
	if ttl > 0 {
		e.expiresAt = time.Now().Add(ttl)
	}
	s.data[key] = e
	s.lru.touch(key)
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(key, value, ttl)
}

// SetNX sets key only if it does not exist. Returns true if set.
func (s *Store) SetNX(key, value string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.data[key]; ok && !e.expired() {
		return false
	}
	s.set(key, value, ttl)
	return true
}

// SetXX sets key only if it already exists. Returns true if set.
func (s *Store) SetXX(key, value string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok || e.expired() {
		return false
	}
	s.set(key, value, ttl)
	return true
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.get(key)
	if !ok {
		return "", false
	}
	s.lru.touch(key)
	return e.value, true
}

func (s *Store) GetDel(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.get(key)
	if !ok {
		return "", false
	}
	val := e.value
	delete(s.data, key)
	s.lru.remove(key)
	return val, true
}

func (s *Store) GetSet(key, value string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var old string
	var had bool
	if e, ok := s.get(key); ok {
		old = e.value
		had = true
	}
	s.set(key, value, 0)
	return old, had
}

func (s *Store) Append(key, value string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.get(key); ok {
		e.value += value
		s.lru.touch(key)
		return int64(len(e.value))
	}
	s.set(key, value, 0)
	return int64(len(value))
}

func (s *Store) StrLen(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e, ok := s.get(key); ok {
		return int64(len(e.value))
	}
	return 0
}

func (s *Store) Incr(key string) (int64, error) {
	return s.IncrBy(key, 1)
}

func (s *Store) IncrBy(key string, n int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cur int64
	if e, ok := s.get(key); ok {
		v, err := strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			return 0, errors.New("ERR value is not an integer or out of range")
		}
		cur = v
	}
	cur += n
	ttl := time.Duration(0)
	if e, ok := s.data[key]; ok && !e.expiresAt.IsZero() {
		ttl = time.Until(e.expiresAt)
		if ttl < 0 {
			ttl = 0
		}
	}
	s.set(key, strconv.FormatInt(cur, 10), ttl)
	return cur, nil
}

func (s *Store) Decr(key string) (int64, error) {
	return s.DecrBy(key, 1)
}

func (s *Store) DecrBy(key string, n int64) (int64, error) {
	return s.IncrBy(key, -n)
}

func (s *Store) MSet(pairs map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range pairs {
		s.set(k, v, 0)
	}
}

func (s *Store) MGet(keys []string) []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		if e, ok := s.get(k); ok {
			result[i] = e.value
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Key commands
// ---------------------------------------------------------------------------

func (s *Store) Del(keys ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int64
	for _, k := range keys {
		if e, ok := s.data[k]; ok && !e.expired() {
			count++
		}
		delete(s.data, k)
		s.lru.remove(k)
	}
	return count
}

func (s *Store) Exists(keys ...string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var count int64
	for _, k := range keys {
		if _, ok := s.get(k); ok {
			count++
		}
	}
	return count
}

func (s *Store) Expire(key string, d time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.get(key); !ok {
		return false
	}
	s.data[key].expiresAt = time.Now().Add(d)
	return true
}

func (s *Store) ExpireAt(key string, t time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.get(key); !ok {
		return false
	}
	s.data[key].expiresAt = t
	return true
}

func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.get(key)
	if !ok {
		return false
	}
	if e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	return true
}

// TTL returns -1*time.Second for no expiry, -2*time.Second for not found.
func (s *Store) TTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.get(key)
	if !ok {
		return -2 * time.Second
	}
	if e.expiresAt.IsZero() {
		return -1 * time.Second
	}
	remaining := time.Until(e.expiresAt)
	if remaining < 0 {
		return -2 * time.Second
	}
	return remaining
}

// PTTL returns -1*time.Millisecond for no expiry, -2*time.Millisecond for not found.
func (s *Store) PTTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.get(key)
	if !ok {
		return -2 * time.Millisecond
	}
	if e.expiresAt.IsZero() {
		return -1 * time.Millisecond
	}
	remaining := time.Until(e.expiresAt)
	if remaining < 0 {
		return -2 * time.Millisecond
	}
	return remaining
}

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k := range s.data {
		if e := s.data[k]; e.expired() {
			continue
		}
		matched, err := path.Match(pattern, k)
		if err != nil {
			continue
		}
		if matched {
			keys = append(keys, k)
		}
	}
	return keys
}

func (s *Store) FlushAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]*entry)
	s.lru = newLRUList(s.lru.maxKeys)
}

func (s *Store) DBSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var count int64
	for _, e := range s.data {
		if !e.expired() {
			count++
		}
	}
	return count
}

func (s *Store) RandomKey() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for k, e := range s.data {
		if !e.expired() {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return "", false
	}
	return keys[rand.Intn(len(keys))], true
}

func (s *Store) Rename(oldKey, newKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.get(oldKey)
	if !ok {
		return errors.New("ERR no such key")
	}
	val := e.value
	expiry := e.expiresAt
	delete(s.data, oldKey)
	s.lru.remove(oldKey)
	ne := &entry{value: val, expiresAt: expiry}
	s.data[newKey] = ne
	s.lru.touch(newKey)
	return nil
}

func (s *Store) RenameNX(oldKey, newKey string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.get(oldKey)
	if !ok {
		return false
	}
	if _, exists := s.get(newKey); exists {
		return false
	}
	val := e.value
	expiry := e.expiresAt
	delete(s.data, oldKey)
	s.lru.remove(oldKey)
	ne := &entry{value: val, expiresAt: expiry}
	s.data[newKey] = ne
	s.lru.touch(newKey)
	return true
}

func (s *Store) Type(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.get(key); ok {
		return "string"
	}
	return "none"
}
