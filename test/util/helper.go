package util

import "sync"

type MessageHist struct {
	mutex   *sync.Mutex
	history []string
	data    map[string]bool
}

func (m *MessageHist) Insert(message string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.history = append(m.history, message)
	m.data[message] = true
}

func (m *MessageHist) Contains(message string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, ok := m.data[message]
	return ok
}

func (m *MessageHist) Values() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var copied []string
	copied = append(copied, m.history...)
	return copied
}

func (m *MessageHist) Size() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.history)
}

func (m *MessageHist) Compare(other MessageHist) int {
	// if both objects hold the same mutex a deadlock will be created.
	if m.mutex == other.mutex {
		return 0
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()
	different := 0
	for i, s := range other.Values() {
		if len(m.history)-1 < i {
			different += 1
			continue
		}

		if m.history[i] != s {
			different += 1
		}
	}
	return different
}

func NewHistory() *MessageHist {
	return &MessageHist{
		mutex: &sync.Mutex{},
		data:  make(map[string]bool),
	}
}
