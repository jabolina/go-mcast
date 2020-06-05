package concurrent

import (
	"go-mcast/pkg/mcast/concurrent"
	"testing"
	"time"
)

func TestDetector_NoStarvation(t *testing.T) {
	timeout := time.Second
	detector := concurrent.NewDetector(timeout)
	ev := uint64(1)
	ch := make(chan bool, 1)

	go func() {
		detector.Happened(ev)
		time.Sleep(timeout / 2)
		ok, _ := detector.Happened(ev)
		ch <- ok
	}()

	select {
	case ok := <-ch:
		if !ok {
			t.Fatalf("expected not exceeded")
		}
	case <-time.After(timeout):
		t.Fatalf("no starvation not completed on timeout %d", timeout)
	}
}

func TestDetector_RoutineStarvation(t *testing.T) {
	timeout := time.Second
	detector := concurrent.NewDetector(timeout)
	ev := uint64(1)
	ch := make(chan bool, 1)

	go func() {
		detector.Happened(ev)
		time.Sleep(timeout)
		ok, _ := detector.Happened(ev)
		ch <- ok
	}()

	select {
	case ok := <-ch:
		if ok {
			t.Fatalf("expected to starvate")
		}
	case <-time.After(timeout + 25 * time.Millisecond):
		t.Fatalf("starvation not completed on timeout %s", timeout)
	}
}
