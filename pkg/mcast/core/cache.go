package core

import (
	"context"
	"sync/atomic"
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
	// Lock for non-blocking actions.
	lock int32

	// Holds the information.
	data map[string]time.Time

	// Parent context.
	ctx context.Context
}

func NewTtlCache(ctx context.Context) Cache {
	c := &TtlCache{
		lock: 0,
		data: make(map[string]time.Time),
		ctx:  ctx,
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
	if atomic.CompareAndSwapInt32(&t.lock, 0x0, 0x1) {
		now := time.Now()
		for key, at := range t.data {
			if now.Sub(at) >= 10*time.Minute {
				delete(t.data, key)
			}
		}
		t.lock = 0x0
	}
}

// Try to add a new value to the cache. What will happen if the
// atomic value is not 0x0?
func (t *TtlCache) Set(id string) {
	if atomic.CompareAndSwapInt32(&t.lock, 0x0, 0x1) {
		if !t.Contains(id) {
			t.data[id] = time.Now()
			t.lock = 0x0
		}
	}
}

// Unsafely verify if a value exists on the cache.
// This can result in a data race, depending on who called.
func (t *TtlCache) Contains(id string) bool {
	_, ok := t.data[id]
	return ok
}
