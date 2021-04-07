package core

import (
	"github.com/coocood/freecache"
)

type Cache interface {
	// Add a new value to the cache.
	// Returns true if the value did not
	// exists previously and false otherwise.
	Set(id string) bool

	// Verify if the cache contains information for
	// the given key.
	Contains(id string) bool
}

// A cache structure where the keys have a TTL.
type TtlCache struct {
	// Holds the cache structure.
	delegate *freecache.Cache
}

func NewTtlCache() Cache {
	c := &TtlCache{
		delegate: freecache.NewCache(10 * 1024 * 1024),
	}
	return c
}

// Try to add a new value to the cache.
func (t *TtlCache) Set(id string) bool {
	old, err := t.delegate.GetOrSet([]byte(id), []byte{0x1}, 500)
	return old == nil && err == nil
}

// Unsafely verify if a value exists on the cache.
// This can result in a data race, depending on who called.
func (t *TtlCache) Contains(id string) bool {
	v, err := t.delegate.Peek([]byte(id))
	return v != nil && err == nil
}
