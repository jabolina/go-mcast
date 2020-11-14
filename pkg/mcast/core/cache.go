package core

import (
	"context"
	"sync"
	"time"
)

type Cache interface {
	// Add a new value to the cache.
	Set(id string)

	// Verify if the cache contains information for
	// the given key.
	Contains(id string) bool
}

// A cache structure where the keys have a TTL.
type TtlCache struct {
	// Mutex for non-blocking actions.
	mutex *sync.Mutex

	// Holds the information.
	data map[string]time.Time

	// Parent context.
	ctx context.Context
}

func NewTtlCache(ctx context.Context) Cache {
	c := &TtlCache{
		mutex: &sync.Mutex{},
		data:  make(map[string]time.Time),
		ctx:   ctx,
	}
	InvokerInstance().Spawn(c.poll)
	return c
}

// Will keep running while the context is open.
// Every minute, this method will try to remove
// the expired cache keys.
func (t *TtlCache) poll() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-time.After(time.Minute):
			t.cleanExpired()
		}
	}
}

// Clean expired cache keys if possible.
// See that is not crucial that remove the elements when the
// method is called, they can be removed a minute later.
func (t *TtlCache) cleanExpired() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	now := time.Now()
	for key, at := range t.data {
		if now.Sub(at) >= 10*time.Minute {
			delete(t.data, key)
		}
	}
}

// Try to add a new value to the cache.
func (t *TtlCache) Set(id string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if !t.lockedContains(id) {
		t.data[id] = time.Now()
	}
}

// Unsafely verify if a value exists on the cache.
// This can result in a data race, depending on who called.
func (t *TtlCache) Contains(id string) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.lockedContains(id)
}

func (t *TtlCache) lockedContains(id string) bool {
	_, ok := t.data[id]
	return ok
}
