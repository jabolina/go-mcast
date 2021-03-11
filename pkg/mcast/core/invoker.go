package core

import "sync"

var (
	// Ensure thread safety while creating a new Invoker.
	create = sync.Once{}

	// Global instance to invoke go routines through the application.
	globalInvoker Invoker
)

// Invoker is responsible for handling goroutines.
// This is used so go routines do not leak and are
// spawned without any control.
// Using the invoker to spawn new routines will guarantee
// that any routine that is not controller careful will
// be known when the application finishes.
type Invoker interface {
	// Spawn a new goroutine and manage through the SyncGroup.
	// This is used to ensure that go routines do not leak.
	Spawn(func())

	// Stop the invoker, after this any invoked go routine
	// will panic.
	Stop()
}

// A singleton struct that implements the Invoker interface.
type SingletonInvoker struct {
	// Use to synchronize if the invoker if open or not.
	mutex *sync.Mutex

	// Flag that tells if the invoker still available or not.
	working bool

	// Wait group to keep track of go routines.
	group *sync.WaitGroup
}

// Create a singleton instance for the Invoker struct.
// This is a singleton to ensure that throughout the
// application exists only one single point where
// go routines are spawned, thus avoiding a leak.
func InvokerInstance() Invoker {
	create.Do(func() {
		globalInvoker = &SingletonInvoker{
			mutex:   &sync.Mutex{},
			working: true,
			group:   &sync.WaitGroup{},
		}
	})
	return globalInvoker
}

// This method will increase the size of the group
// count and spawn the new go routine. After the
// routine is done, the group will be decreased.
//
// This method will panic if the invoker is already closed.
func (c *SingletonInvoker) Spawn(f func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.working {
		panic("invoker already closed!")
	}

	c.group.Add(1)
	go func() {
		defer c.group.Done()
		f()
	}()
}

// Blocks while waiting for go routines to stop.
// This will priorityQueue the working mode to off, so after
// this is called any spawned go routine will panic.
func (c *SingletonInvoker) Stop() {
	c.mutex.Lock()
	c.working = false
	c.mutex.Unlock()
	c.group.Wait()
}
