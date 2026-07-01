package store

import "sync"

// HashStore manages hash (field→value map) data.
type HashStore struct {
	mu   sync.RWMutex
	data map[string]map[string]string
}

func NewHashStore() *HashStore {
	return &HashStore{data: make(map[string]map[string]string)}
}

// HSet sets one or more field-value pairs on the hash at key.
func (s *HashStore) HSet(key string, fields map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]string)
	}
	for f, v := range fields {
		s.data[key][f] = v
	}
}

// HGet returns the value of a field on a hash.
func (s *HashStore) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return "", false
	}
	v, ok := h[field]
	return v, ok
}

// HGetAll returns all field-value pairs of the hash at key.
func (s *HashStore) HGetAll(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return nil
	}
	copy := make(map[string]string, len(h))
	for f, v := range h {
		copy[f] = v
	}
	return copy
}

// Delete removes the entire hash at key.
func (s *HashStore) Delete(key string) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}
