package store

import "sync"

// StringStore manages string key-value pairs.
type StringStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStringStore() *StringStore {
	return &StringStore{data: make(map[string]string)}
}

// SetNX sets key to value only if key does not already exist.
// Returns true if the key was set, false if it already existed.
func (s *StringStore) SetNX(key, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.data[key]; exists {
		return false
	}
	s.data[key] = value
	return true
}

// Get returns the value and true if key exists, or empty string and false.
func (s *StringStore) Get(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

// Delete removes a key. Returns true if the key existed.
func (s *StringStore) Delete(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}
