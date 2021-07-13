package hpq

import (
	"github.com/coocood/freecache"
)

var (
	defaultValue    = []byte{0x1}
	entryExpiration = 500
)

type Purgatory interface {
	// Set will add a new entry to the purgatory.
	// Returns true if the Value did not exists previously
	// and false otherwise.
	Set(id string) bool

	// Contains verify if the given Value exists in purgatory.
	Contains(id string) bool
}

// TtlPurgatory is structure that implements the Purgatory interface.
// On this implementation, all added entries will have a TTL then
// they will be removed from the purgatory.
type TtlPurgatory struct {
	// delegate structure that will handle all entries.
	delegate *freecache.Cache
}

func NewPurgatory() Purgatory {
	c := &TtlPurgatory{
		delegate: freecache.NewCache(10 * 1024 * 1024),
	}
	return c
}

// Set add a new Value to the cache.
// If a previous element already exists, nothing changes.
func (t *TtlPurgatory) Set(id string) bool {
	old, err := t.delegate.GetOrSet([]byte(id), defaultValue, entryExpiration)
	return old == nil && err == nil
}

// Contains verify if the given entry already exists on purgatory.
func (t *TtlPurgatory) Contains(id string) bool {
	v, err := t.delegate.Peek([]byte(id))
	return v != nil && err == nil
}
