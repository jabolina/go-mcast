package internal

import "sync"

// An exchange object to be used when holding information
// about the exchanged timestamps.
type exchanged struct {
	// Which partition sent the timestamp.
	from Partition

	// The timestamp for the partition.
	timestamp uint64
}

// A thread safe struct to hold information locally.
type Memo struct {
	// Synchronization for operations.
	mutex *sync.Mutex

	// Holds information as serialized values for a
	// unique key.
	values map[UID][]exchanged
}

func NewMemo() *Memo {
	return &Memo{
		mutex:  &sync.Mutex{},
		values: make(map[UID][]exchanged),
	}
}

// This method will try to insert the new vote for
// the exchange timestamp onto the exchanged memo.
// If other peer from the origin partition already
// voted for a timestamp than the vote can be ignored,
// since is needed only a single peer from each partition
// to send the timestamp.
func (m *Memo) Insert(key UID, from Partition, value uint64) {
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
	} else {
		skip := false
		for _, e := range m.values[key] {
			if e.from == from {
				skip = true
				break
			}
		}

		if !skip {
			m.values[key] = append(m.values[key], exchanged{
				from:      from,
				timestamp: value,
			})
		}
	}
}

// This method will remove the information
// from the voted timestamp from the memo.
func (m *Memo) Remove(key UID) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.values, key)
}

// This method will return all proposed values
// to a message.
func (m *Memo) Read(key UID) []uint64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var timestamps []uint64
	for _, e := range m.values[key] {
		timestamps = append(timestamps, e.timestamp)
	}
	return timestamps
}
