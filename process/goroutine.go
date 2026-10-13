package process

import (
	"sync"
	"sync/atomic"
)

// tracker counts running goroutines. Unlike a sync.WaitGroup, it lets a goroutine start while wait
// waits, from anywhere: an HTTP handler of a killed server may still call Go during shutdown.
type tracker struct {
	running atomic.Int64
	mu      sync.Mutex
	idle    sync.Cond // broadcast under mu when running drops to 0
}

func newTracker() *tracker {
	t := &tracker{}
	t.idle.L = &t.mu
	return t
}

func (t *tracker) start(fn func()) {
	t.running.Add(1)
	go func() {
		defer t.done()
		fn()
	}()
}

// done locks mu only when running drops to 0. A waiter holds mu from reading running until it
// sleeps, so the broadcast cannot fall between the two.
func (t *tracker) done() {
	if t.running.Add(-1) == 0 {
		t.mu.Lock()
		t.idle.Broadcast()
		t.mu.Unlock()
	}
}

func (t *tracker) wait() {
	t.mu.Lock()
	for t.running.Load() > 0 {
		t.idle.Wait()
	}
	t.mu.Unlock()
}

// tracked holds the goroutines started with Go.
var tracked = newTracker()

// Go runs fn in a new goroutine that WaitGoroutines waits for. Use it instead of a bare go
// statement for background work that must finish before the application closes its resources.
//
// fn must not be nil, and a panic in fn is not recovered: it crashes the process, like one in any
// goroutine.
func Go(fn func()) {
	tracked.start(fn)
}

// WaitGoroutines blocks until no goroutine started with Go runs, including those started while it
// waits. App and CLI call it after their servers or command stopped and before they close their
// resources. It has no timeout, so background work must end on its own.
func WaitGoroutines() {
	tracked.wait()
}
