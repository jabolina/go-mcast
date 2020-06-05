package concurrent

import (
	"context"
	"go-mcast/pkg/mcast/concurrent"
	"testing"
)

func TestScheduler(t *testing.T) {
	scheduler := concurrent.NewScheduler()
	defer scheduler.Stop()

	next := 0
	jobCreator := func(i int) concurrent.Job {
		return func(ctx context.Context) {
			if next != i {
				t.Fatalf("job#%d: got %d, want %d", i, next, i)
			}
			next = i + 1
		}
	}

	var jobs []concurrent.Job
	for i := 0; i < 100; i++ {
		jobs = append(jobs, jobCreator(i))
	}

	for _, j := range jobs {
		scheduler.Schedule(j)
	}

	scheduler.Wait(100)
	if scheduler.Pending() != 0 {
		t.Errorf("scheduled = %d, want 0", scheduler.Pending())
	}
}
