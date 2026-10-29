package process

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunnerTryGo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRunner(NewChannelSemaphore(1))
		release := make(chan struct{})
		if err := r.TryGo(func() { <-release }); err != nil {
			t.Fatalf("TryGo with a free slot: %v", err)
		}

		var ran atomic.Bool
		if err := r.TryGo(func() { ran.Store(true) }); err != ErrNoSlot {
			t.Fatalf("TryGo without a free slot = %v, want ErrNoSlot", err)
		}

		returned := waitInBackground(WaitGoroutines)
		synctest.Wait()
		if returned.Load() {
			t.Fatal("WaitGoroutines returned while a TryGo goroutine was running")
		}

		close(release)
		synctest.Wait()
		if !returned.Load() {
			t.Fatal("WaitGoroutines did not return after the TryGo goroutine ended")
		}
		if ran.Load() {
			t.Fatal("TryGo ran a function it had no slot for")
		}
		if err := r.TryGo(func() {}); err != nil {
			t.Fatalf("TryGo after the slot was freed: %v", err)
		}
		WaitGoroutines()
	})
}

func TestRunnerGoOrRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewChannelSemaphore(1)
		r := NewRunner(s)

		// With a free slot, fn runs in its own goroutine: a synchronous call would block forever.
		release := make(chan struct{})
		r.GoOrRun(func() { <-release })

		// Without one, fn has run when GoOrRun returns.
		var ran bool
		r.GoOrRun(func() { ran = true })
		if !ran {
			t.Fatal("GoOrRun without a free slot did not run fn synchronously")
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
	if err := NewRunner(s).TryGo(func() { ran.Add(1) }); err != errDown {
		t.Fatalf("TryGo = %v, want the semaphore's error", err)
	}
	if err := NewGroup(s).Go(t.Context(), func() { ran.Add(1) }); err != errDown {
		t.Fatalf("Go = %v, want the semaphore's error", err)
	}
	if ran.Load() != 0 {
		t.Fatal("a runner ran a function it had no slot for")
	}

	NewRunner(s).GoOrRun(func() { ran.Add(1) })
	if ran.Load() != 1 {
		t.Fatal("GoOrRun did not run fn synchronously after the semaphore failed")
	}
}

func TestGroupLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const limit, runs = 3, 20
		r := NewGroup(NewChannelSemaphore(limit))
		var running, peak, done atomic.Int32
		for range runs {
			err := r.Go(t.Context(), func() {
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

func TestGroupGoWaitsForASlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewGroup(NewChannelSemaphore(1))
		release := make(chan struct{})
		if err := r.Go(t.Context(), func() { <-release }); err != nil {
			t.Fatal(err)
		}

		var started atomic.Bool
		result := make(chan error, 1)
		go func() { result <- r.Go(t.Context(), func() { started.Store(true) }) }()
		synctest.Wait()
		if started.Load() || len(result) != 0 {
			t.Fatal("Go started a function without a free slot")
		}

		close(release)
		if err := <-result; err != nil {
			t.Fatalf("Go after the slot was freed: %v", err)
		}
		r.Wait()
		if !started.Load() {
			t.Fatal("Wait returned before the second function ran")
		}
	})
}

func TestGroupGoContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewGroup(NewChannelSemaphore(1))
		release := make(chan struct{})
		if err := r.Go(t.Context(), func() { <-release }); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(time.Second, cancel)
		var ran atomic.Bool
		err := r.Go(ctx, func() { ran.Store(true) })
		if !errors.Is(err, ErrNoSlot) || !errors.Is(err, context.Canceled) {
			t.Fatalf("Go = %v, want ErrNoSlot and context.Canceled", err)
		}

		close(release)
		r.Wait()
		if ran.Load() {
			t.Fatal("Go ran a function after its context ended")
		}
	})
}
