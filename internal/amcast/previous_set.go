package amcast

import (
	"go-mcast/pkg/mcast"
	"sync"
)

// Holds information about older messages
type PreviousSet struct {
	mutex  sync.Mutex
	values map[mcast.UID]bool
}

// The conflict relationship is used to compute if the given message unique identifier
// conflicts with any other identifier on the previous set, is used to order the
// requests, used for clock changes and for the delivery process.
type ConflictRelationship interface {
	Conflicts(mcast.UID) bool
}

// The PreviousSet implements the ConflictRelationship interface.
func (ps *PreviousSet) Conflicts(message mcast.UID) bool {
	return true
}

// Add a new message into the previous set
func (ps *PreviousSet) Add(message mcast.UID) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	ps.values[message] = true
}

// Return the values present on the set on the time of the read.
func (ps *PreviousSet) Values() map[mcast.UID]bool {
	ps.mutex.Lock()
	values := ps.values
	ps.mutex.Unlock()
	return values
}

// Clear all entries on the previous set
func (ps *PreviousSet) Clear() {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()

	for k := range ps.values {
		delete(ps.values, k)
	}

	ps.values = make(map[mcast.UID]bool)
}

// Creates a new PreviousSet
func NewPreviousSet() *PreviousSet {
	return &PreviousSet{
		mutex:  sync.Mutex{},
		values: make(map[mcast.UID]bool),
	}
}
