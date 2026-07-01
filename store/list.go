package store

import "sync"

// ListStore manages ordered list (slice of strings) data.
type ListStore struct {
	mu   sync.RWMutex
	data map[string][]string
}

func NewListStore() *ListStore {
	return &ListStore{data: make(map[string][]string)}
}

// RPush appends a value to the tail of the list at key. Returns new length.
func (s *ListStore) RPush(key, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append(s.data[key], value)
	return len(s.data[key])
}

// LPop removes and returns the first element of the list at key.
func (s *ListStore) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.data[key]
	if !ok || len(list) == 0 {
		return "", false
	}
	val := list[0]
	s.data[key] = list[1:]
	if len(s.data[key]) == 0 {
		delete(s.data, key)
	}
	return val, true
}

// LMove atomically removes the first element from src and appends it to dst.
// Returns the moved element and true, or empty string and false if src is empty.
func (s *ListStore) LMove(src, dst string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.data[src]
	if !ok || len(list) == 0 {
		return "", false
	}
	val := list[0]
	s.data[src] = list[1:]
	if len(s.data[src]) == 0 {
		delete(s.data, src)
	}
	s.data[dst] = append(s.data[dst], val)
	return val, true
}

// LRem removes the first occurrence of value from the list at key.
// Returns true if an element was removed.
func (s *ListStore) LRem(key, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.data[key]
	if !ok {
		return false
	}
	for i, v := range list {
		if v == value {
			s.data[key] = append(list[:i], list[i+1:]...)
			if len(s.data[key]) == 0 {
				delete(s.data, key)
			}
			return true
		}
	}
	return false
}

// LRange returns elements from start to stop (inclusive). Negative indices supported.
func (s *ListStore) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.data[key]
	if !ok {
		return nil
	}
	n := len(list)
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return nil
	}
	out := make([]string, stop-start+1)
	copy(out, list[start:stop+1])
	return out
}

// LLen returns the length of the list at key.
func (s *ListStore) LLen(key string) int {
	s.mu.RLock()
	n := len(s.data[key])
	s.mu.RUnlock()
	return n
}
