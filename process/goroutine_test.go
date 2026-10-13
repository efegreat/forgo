package process

import (
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

// waitInBackground calls wait in a new goroutine and reports whether it returned.
func waitInBackground(wait func()) *atomic.Bool {
	var returned atomic.Bool
	go func() {
		wait()
		returned.Store(true)
	}()
	return &returned
}

func TestWaitGoroutinesWithNothingRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		WaitGoroutines()
	})
}

func TestWaitGoroutinesWaitsForGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var ran atomic.Bool
		Go(func() {
			<-release
			ran.Store(true)
		})

		returned := waitInBackground(WaitGoroutines)
		synctest.Wait()
		if returned.Load() {
			t.Fatal("WaitGoroutines returned while a goroutine was running")
		}

		close(release)
		synctest.Wait()
		if !returned.Load() || !ran.Load() {
			t.Fatalf("returned = %v, ran = %v, want both after the goroutine ended", returned.Load(), ran.Load())
		}
	})
}

// An HTTP handler of a killed server may call Go while App waits. The new goroutine must be waited
// for too, even when it starts from an untracked goroutine.
func TestWaitGoroutinesWaitsForGoroutinesStartedDuringTheWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, second := make(chan struct{}), make(chan struct{})
		Go(func() { <-first })
		returned := waitInBackground(WaitGoroutines)
		synctest.Wait()

		Go(func() { <-second })
		close(first)
		synctest.Wait()
		if returned.Load() {
			t.Fatal("WaitGoroutines returned while a goroutine started during the wait was running")
		}

		close(second)
		synctest.Wait()
		if !returned.Load() {
			t.Fatal("WaitGoroutines did not return after every goroutine ended")
		}
	})
}

func TestWaitGoroutinesWaitsForNestedGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		Go(func() {
			Go(func() { <-release })
		})
		synctest.Wait()

		returned := waitInBackground(WaitGoroutines)
		synctest.Wait()
		if returned.Load() {
			t.Fatal("WaitGoroutines returned while a nested goroutine was running")
		}

		close(release)
		synctest.Wait()
		if !returned.Load() {
			t.Fatal("WaitGoroutines did not return after the nested goroutine ended")
		}
	})
}

// With a sync.WaitGroup, Go concurrent with WaitGoroutines is a data race and panics with "WaitGroup
// is reused before previous Wait has returned" within a few hundred thousand rounds.
func TestGoConcurrentWithWaitGoroutines(t *testing.T) {
	rounds := 20000
	if testing.Short() {
		rounds = 1000
	}
	var starters sync.WaitGroup
	for range rounds {
		Go(func() {})
		starters.Go(func() { Go(func() {}) })
		WaitGoroutines()
	}
	starters.Wait()
	WaitGoroutines()
}

func TestTrackerWaitsUntilNoneRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tr := newTracker()
		releases := make([]chan struct{}, 3)
		for i := range releases {
			releases[i] = make(chan struct{})
			tr.start(func() { <-releases[i] })
		}
		returned := waitInBackground(tr.wait)

		for i, release := range releases {
			synctest.Wait()
			if returned.Load() {
				t.Fatalf("wait returned with %d goroutines running", len(releases)-i)
			}
			close(release)
		}
		synctest.Wait()
		if !returned.Load() {
			t.Fatal("wait did not return after every goroutine ended")
		}
		// The tracker is reusable, and tracks nothing more.
		tr.wait()
	})
}
