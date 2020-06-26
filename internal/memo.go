package internal

import "sync"

// A thread safe struct to hold information locally.
type Memo struct {
	// Synchronization for operations.
	mutex *sync.Mutex

	// Holds information as serialized values for a
	// unique key.
	values map[UID][]uint64
}

func NewMemo() *Memo {
	return &Memo{
		mutex:  &sync.Mutex{},
		values: make(map[UID][]uint64),
	}
}

func (m *Memo) Insert(key UID, value uint64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, exists := m.values[key]
	if !exists {
		m.values[key] = []uint64{value}
	} else {
		m.values[key] = append(m.values[key], value)
	}
}

func (m *Memo) Remove(key UID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.values, key)
}

func (m *Memo) Read(key UID) []uint64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.values[key]
}
