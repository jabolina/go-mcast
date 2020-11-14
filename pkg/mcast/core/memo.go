package core

import (
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

// An exchange object to be used when holding information
// about the exchanged timestamps.
type exchanged struct {
	// Which partition sent the timestamp.
	from types.Partition

	// The timestamp for the partition.
	timestamp uint64
}

// A thread safe struct to hold information locally.
type Memo struct {
	// Synchronization for operations.
	mutex *sync.Mutex

	// Holds information as serialized values for a
	// unique key.
	values map[types.UID][]exchanged
}

func NewMemo() *Memo {
	return &Memo{
		mutex:  &sync.Mutex{},
		values: make(map[types.UID][]exchanged),
	}
}

// This method will try to insert the new vote for
// the exchange timestamp onto the exchanged memo.
// If other peer from the origin partition already
// voted for a timestamp than the vote can be ignored,
// since is needed only a single peer from each partition
// to send the timestamp.
func (m *Memo) Insert(key types.UID, from types.Partition, value uint64) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	_, exists := m.values[key]
	if !exists {
		m.values[key] = []exchanged{
			{
				from:      from,
				timestamp: value,
			},
		}
		return true
	}

	for _, e := range m.values[key] {
		if e.from == from {
			return false
		}
	}

	m.values[key] = append(m.values[key], exchanged{
		from:      from,
		timestamp: value,
	})
	return true
}

func (m *Memo) Show(at string, key types.UID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	fmt.Printf("%s @ %s -> %#v\n", key, at, m.values[key])
}

// This method will remove the information
// from the voted timestamp from the memo.
func (m *Memo) Remove(key types.UID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.values, key)
}

// This method will return all proposed values
// to a message.
func (m *Memo) Read(key types.UID) []uint64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var timestamps []uint64
	for _, e := range m.values[key] {
		timestamps = append(timestamps, e.timestamp)
	}
	return timestamps
}
