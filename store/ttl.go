package store

import (
	"context"
	"time"
)

func (s *Store) startExpirer(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.deleteExpired()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) deleteExpired() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, entry := range s.data {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(s.data, key)
			s.lru.remove(key)
		}
	}
}
