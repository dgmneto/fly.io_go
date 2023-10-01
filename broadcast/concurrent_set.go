package main

import (
	"sync"
)

type ConcurrentSet struct {
	mu sync.RWMutex
	items map[int64]struct{}
}

func NewConcurrentSet() *ConcurrentSet {
	return &ConcurrentSet{
		items: make(map[int64]struct{}),
	}
}

func (s *ConcurrentSet) Add(item int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[item] = struct{}{}
}

func (s *ConcurrentSet) Contains(item int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.items[item]
	return exists
}

func (s *ConcurrentSet) AllItems() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]int64, len(s.items))
	i := 0
	for item := range s.items {
		items[i] = item
		i += 1
	}
	return items
}