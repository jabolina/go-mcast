package protocol

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

// An exchange object to be used when holding information
// about the ballot timestamps.
type ballot struct {
	// Which partition sent the timestamp.
	from types.Partition

	// The timestamp for the partition.
	timestamp uint64
}

// BallotBox is a thread safe struct to hold information about sequence number voting.
type BallotBox struct {
	// Synchronization for operations.
	mutex *sync.Mutex

	// Holds information as serialized votes for a unique key.
	votes map[types.UID][]ballot
}

func NewBallotBox() *BallotBox {
	return &BallotBox{
		mutex: &sync.Mutex{},
		votes: make(map[types.UID][]ballot),
	}
}

func (b *BallotBox) newFilledBallot(voter types.Partition, vote uint64) ballot {
	return ballot{
		from:      voter,
		timestamp: vote,
	}
}

// Insert will try to insert the new vote for the exchange timestamp onto the ballot box.
// If other peer from the origin partition already voted for a timestamp than the vote can
// be ignored, since is needed only a single peer from each partition
// to send the timestamp.
func (b *BallotBox) Insert(key types.UID, from types.Partition, value uint64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	_, exists := b.votes[key]
	if !exists {
		b.votes[key] = []ballot{b.newFilledBallot(from, value)}
		return
	}

	b.votes[key] = append(b.votes[key], b.newFilledBallot(from, value))
}

// Remove will remove the votes timestamp from the ballot box.
func (b *BallotBox) Remove(key types.UID) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	delete(b.votes, key)
}

// This method will return all proposed votes to a message.
func (b *BallotBox) Read(key types.UID) []uint64 {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	var timestamps []uint64
	for _, e := range b.votes[key] {
		timestamps = append(timestamps, e.timestamp)
	}
	return timestamps
}

func (b *BallotBox) ElectionSize(key types.UID) int {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	ballots, exists := b.votes[key]
	if !exists {
		return 0
	}

	set := make(map[types.Partition]int)
	for _, bl := range ballots {
		set[bl.from] = 1
	}

	return len(set)
}
