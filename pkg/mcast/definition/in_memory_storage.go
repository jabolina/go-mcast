package definition

import (
	"fmt"
	"sync"
)

// Provides a basic implementation of the Storage interface
// that will use only the memory, no stable storage is provided
// with this implementation. Is up to the user to use its desired storage.
type InMemoryStorage struct {
	// Mutex for operations executions
	mutex *sync.Mutex

	// The in-memory storage
	kv map[string][]byte
}

// Implements the Set for the Storage interface
func (s *InMemoryStorage) Set(key []byte, value []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.kv[string(key)] = value
	return nil
}

// Implements the Get for the Storage interface.
// On this implementation if no value was found, an error will be returned.
func (s *InMemoryStorage) Get(key []byte) ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	value, ok := s.kv[string(key)]
	if !ok {
		return nil, fmt.Errorf("not found value for %s", string(key))
	}
	return value, nil
}

// Create a new storage using memory only.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		mutex: &sync.Mutex{},
		kv:    make(map[string][]byte),
	}
}
