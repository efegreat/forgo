package process

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunnerDoAsync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRunner(NewChannelSemaphore(1))
		release := make(chan struct{})
		if err := r.DoAsync(func() { <-release }); err != nil {
			t.Fatalf("DoAsync with a free slot: %v", err)
		}

		var ran atomic.Bool
		if err := r.DoAsync(func() { ran.Store(true) }); err != ErrNoSlot {
			t.Fatalf("DoAsync without a free slot = %v, want ErrNoSlot", err)
		}

		returned := waitInBackground(WaitGoroutines)
		synctest.Wait()
		if returned.Load() {
			t.Fatal("WaitGoroutines returned while a DoAsync goroutine was running")
		}

		close(release)
		synctest.Wait()
		if !returned.Load() {
			t.Fatal("WaitGoroutines did not return after the DoAsync goroutine ended")
		}
		if ran.Load() {
			t.Fatal("DoAsync ran a function it had no slot for")
		}
		if err := r.DoAsync(func() {}); err != nil {
			t.Fatalf("DoAsync after the slot was freed: %v", err)
		}
		WaitGoroutines()
	})
}

func TestRunnerTryAsync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewChannelSemaphore(1)
		r := NewRunner(s)

		// With a free slot, fn runs in its own goroutine: a synchronous call would block forever.
		release := make(chan struct{})
		r.TryAsync(func() { <-release })

		// Without one, fn has run when TryAsync returns.
		var ran bool
		r.TryAsync(func() { ran = true })
		if !ran {
			t.Fatal("TryAsync without a free slot did not run fn synchronously")
		}

		close(release)
		WaitGoroutines()
		if err := s.Acquire(); err != nil {
			t.Fatalf("the slot was not freed: %v", err)
		}
	})
}

// failingSemaphore never has a slot, and fails with err.
type failingSemaphore struct{ err error }

func (s failingSemaphore) Acquire() error                    { return s.err }
func (s failingSemaphore) WaitAcquire(context.Context) error { return s.err }
func (s failingSemaphore) Release()                          { panic("released without a slot") }

func TestRunnersReturnTheSemaphoreError(t *testing.T) {
	errDown := errors.New("semaphore down")
	s := failingSemaphore{err: errDown}

	var ran atomic.Int32
	if err := NewRunner(s).DoAsync(func() { ran.Add(1) }); err != errDown {
		t.Fatalf("DoAsync = %v, want the semaphore's error", err)
	}
	if err := NewConcurrentRunner(s).Run(t.Context(), func() { ran.Add(1) }); err != errDown {
		t.Fatalf("Run = %v, want the semaphore's error", err)
	}
	if ran.Load() != 0 {
		t.Fatal("a runner ran a function it had no slot for")
	}

	NewRunner(s).TryAsync(func() { ran.Add(1) })
	if ran.Load() != 1 {
		t.Fatal("TryAsync did not run fn synchronously after the semaphore failed")
	}
}

func TestConcurrentRunnerLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit, runs = 3, 20
		r := NewConcurrentRunner(NewChannelSemaphore(limit))
		var running, peak, done atomic.Int32
		for range runs {
			err := r.Run(t.Context(), func() {
				n := running.Add(1)
				for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
				}
				time.Sleep(time.Second)
				running.Add(-1)
				done.Add(1)
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		r.Wait()

		if done.Load() != runs {
			t.Fatalf("Wait returned after %d of %d functions", done.Load(), runs)
		}
		if peak.Load() != limit {
			t.Fatalf("%d functions ran at once, want %d", peak.Load(), limit)
		}
	})
}

func TestConcurrentRunnerRunWaitsForASlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewConcurrentRunner(NewChannelSemaphore(1))
		release := make(chan struct{})
		if err := r.Run(t.Context(), func() { <-release }); err != nil {
			t.Fatal(err)
		}

		var started atomic.Bool
		result := make(chan error, 1)
		go func() { result <- r.Run(t.Context(), func() { started.Store(true) }) }()
		synctest.Wait()
		if started.Load() || len(result) != 0 {
			t.Fatal("Run started a function without a free slot")
		}

		close(release)
		if err := <-result; err != nil {
			t.Fatalf("Run after the slot was freed: %v", err)
		}
		r.Wait()
		if !started.Load() {
			t.Fatal("Wait returned before the second function ran")
		}
	})
}

func TestConcurrentRunnerRunContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewConcurrentRunner(NewChannelSemaphore(1))
		release := make(chan struct{})
		if err := r.Run(t.Context(), func() { <-release }); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(time.Second, cancel)
		var ran atomic.Bool
		err := r.Run(ctx, func() { ran.Store(true) })
		if !errors.Is(err, ErrNoSlot) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Run = %v, want ErrNoSlot and context.Canceled", err)
		}

		close(release)
		r.Wait()
		if ran.Load() {
			t.Fatal("Run ran a function after its context ended")
		}
	})
}
