// Package observertest records what middleware.Observe tells an operation.IObserver, for the tests of every
// server. Only tests import it.
package observertest

import (
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
)

// Observer is an operation.IObserver that records every call. It is safe for concurrent use.
type Observer struct {
	// State, when set, returns what End records of the operation's State, read when End is called, so that a
	// test sees the State as the observer does.
	State func(st any) any

	mu      sync.Mutex
	running map[*operation.Context]bool
	starts  int
	ends    []End
	changed chan struct{}
}

// End is what an Observer recorded of a call of End.
type End struct {
	Type, Name string
	operation.Observation
	// State is what Observer.State returned, nil without it.
	State any
	// Started reports whether Start was called for the operation, and End was not yet.
	Started bool
}

// Provider returns a provider of o, for forgo.NewBootstrap.
func (o *Observer) Provider() func() operation.IObserver {
	return func() operation.IObserver { return o }
}

func (o *Observer) Start(ctx *operation.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.running == nil {
		o.running = make(map[*operation.Context]bool)
	}
	o.running[ctx] = true
	o.starts++
}

func (o *Observer) End(ctx *operation.Context, obs operation.Observation) {
	e := End{Type: ctx.Type, Name: ctx.Name, Observation: obs}
	if o.State != nil {
		e.State = o.State(ctx.State)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	e.Started = o.running[ctx]
	delete(o.running, ctx)
	o.ends = append(o.ends, e)
	if o.changed != nil {
		close(o.changed)
		o.changed = nil
	}
}

// Starts returns the number of calls of Start.
func (o *Observer) Starts() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.starts
}

// Ends returns the calls of End so far, in order.
func (o *Observer) Ends() []End {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]End(nil), o.ends...)
}

// Wait waits for n calls of End, and returns them. It fails t after 5 seconds.
func (o *Observer) Wait(t testing.TB, n int) []End {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		o.mu.Lock()
		if len(o.ends) >= n {
			ends := append([]End(nil), o.ends...)
			o.mu.Unlock()
			return ends
		}
		if o.changed == nil {
			o.changed = make(chan struct{})
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("observed %d operations, want %d", len(o.Ends()), n)
		}
	}
}

// Last returns the last End, after waiting for it to be the nth one. It fails t when there are more than n.
func (o *Observer) Last(t testing.TB, n int) End {
	t.Helper()
	ends := o.Wait(t, n)
	if len(ends) != n {
		t.Fatalf("observed %d operations, want %d", len(ends), n)
	}
	e := ends[n-1]
	if !e.Started {
		t.Fatalf("End of %s %q without Start", e.Type, e.Name)
	}
	return e
}
