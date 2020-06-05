package concurrent

import (
	"context"
	"sync"
)

// An issued job to be executed
type Job func(ctx context.Context)

type Scheduler interface {
	// Schedule a job for execution.
	Schedule(Job)

	// How many jobs are pending.
	Pending() int

	// Wait up to the number of given jobs to be completed.
	Wait(int)

	// Stop the scheduler.
	Stop()
}

type fifo struct {
	mutex sync.Mutex

	ch chan struct{}
	completed int
	pending []Job

	ctx context.Context
	cancellable context.CancelFunc

	finishes *sync.Cond
	close chan struct{}
}

func NewScheduler() Scheduler {
	s := &fifo{
		ch: make(chan struct{}, 1),
		close: make(chan struct{}, 1),
	}

	s.finishes = sync.NewCond(&s.mutex)
	s.ctx, s.cancellable = context.WithCancel(context.Background())
	go s.forever()
	return s
}

// Schedule the job to be executed sometime in the future.
func (s *fifo) Schedule(j Job) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.cancellable == nil {
		panic("scheduler is already stopped")
	}

	if len(s.pending) == 0 {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
	s.pending = append(s.pending, j)
}

// How many jobs are still pending.
func (s *fifo) Pending() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return len(s.pending)
}

// Wait up to n jobs to finishes before returning.
func (s *fifo) Wait(how int) {
	s.finishes.L.Lock()
	defer s.finishes.L.Unlock()

	for s.completed < how || len(s.pending) != 0 {
		s.finishes.Wait()
	}
}

// Stop the current Scheduler. Any job that tries to be scheduled
// will panic.
func (s *fifo) Stop() {
	s.mutex.Lock()
	s.cancellable()
	s.cancellable = nil
	s.mutex.Unlock()
	<-s.close
}

// Keeps polling the scheduled jobs for execution forever
func (s *fifo) forever()  {
	defer func() {
		close(s.close)
		close(s.ch)
	}()

	for {
		var job Job
		s.mutex.Lock()
		if len(s.pending) != 0 {
			job = s.pending[0]
		}
		s.mutex.Unlock()

		if job == nil {
			select {
			case <-s.ch:
			case <-s.ctx.Done():
				s.mutex.Lock()
				jobs := s.pending
				s.mutex.Unlock()
				for _, job := range jobs {
					job(s.ctx)
				}
				return
			}
		} else {
			job(s.ctx)
			s.finishes.L.Lock()
			s.completed++
			s.pending = s.pending[1:]
			s.finishes.Broadcast()
			s.finishes.L.Unlock()
		}
	}
}
