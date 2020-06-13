package mcast

import (
	"hash/fnv"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Holds information about older messages.
type PreviousSet struct {
	mutex  *sync.Mutex
	values map[uint64]UID
	size   uint64
	p      uint64
	a      uint64
	b      uint64
}

// The conflict relationship is used to compute if the given message unique identifier
// conflicts with any other identifier on the previous set, is used to order the
// requests, used for clock changes and for the delivery process.
type ConflictRelationship interface {
	// Verify if the given addresses conflicts with the values already
	// present on memory.
	Conflicts(id []ServerAddress) bool

	// Verify if the given id conflicts with values given at the sample.
	ConflictsWith(id []ServerAddress, sample map[UID][]ServerAddress) bool
}

// Using a universal hash function, hashes the received UID.
func (h *PreviousSet) hash(destinations []ServerAddress) uint64 {
	hasher := fnv.New64()
	for _, v := range destinations {
		_, err := hasher.Write([]byte(v))
		if err != nil {
			return 0
		}
	}

	code := hasher.Sum64()
	return ((h.a*code + h.b) % h.p) % h.size
}

// The PreviousSet implements the ConflictRelationship interface.
// Will be used a universal hash function to verify the collision between requests,
// if the hash generated from the message destination collides, that will indicate that
// the messages conflicts, since exists another request for the same region being
// processed.
func (ps *PreviousSet) Conflicts(id []ServerAddress) bool {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	code := ps.hash(id)
	_, ok := ps.values[code]
	return ok
}

func (h *PreviousSet) ConflictsWith(id []ServerAddress, sample map[UID][]ServerAddress) bool {
	hashed := make(map[uint64]UID)
	for uid, addresses := range sample {
		code := h.hash(addresses)
		hashed[code] = uid
	}

	code := h.hash(id)
	_, ok := hashed[code]
	return ok
}

// Add a new message into the previous set
func (ps *PreviousSet) Add(destination []ServerAddress, uid UID) {
	ps.mutex.Lock()
	defer ps.mutex.Unlock()
	ps.values[ps.hash(destination)] = uid
}

// Return the values present on the set on the time of the read.
func (ps *PreviousSet) Values() map[uint64]UID {
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

	ps.values = make(map[uint64]UID)
}

// Creates a new PreviousSet
func NewPreviousSet() *PreviousSet {
	source := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(source)
	size := int64(math.Abs(float64(generator.Int63n(math.MaxInt64 / 2))))
	p, a, b := generatePrimes(generator, size)

	return &PreviousSet{
		mutex:  &sync.Mutex{},
		values: make(map[uint64]UID),
		size:   uint64(size),
		p:      p,
		a:      a,
		b:      b,
	}
}

// The universal hash function needs the use of random generated prime numbers.
// This function will generated the needed numbers.
func generatePrimes(generator *rand.Rand, size int64) (uint64, uint64, uint64) {
	seed := generator.Int63n(2 * size)
	for seed < size || !isPrime(seed) {
		seed = generator.Int63n(2 * size)
	}

	a := uint64(generator.Int63n(seed))
	b := uint64(generator.Int63n(seed))
	for b == a {
		b = uint64(generator.Int63n(seed))
	}
	return uint64(seed), a, b
}

// Verifies if the given number is prime or not.
func isPrime(value int64) bool {
	if value <= 3 || value%2 == 0 {
		return false
	}

	divisor := int64(3)
	for (divisor <= int64(math.Sqrt(float64(value)))) && (value%divisor != 0) {
		divisor += 2
	}

	return value%divisor != 0
}
