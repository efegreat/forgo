// Package observertest records what the servers tell an operation.IObserver and the extension interfaces
// of the operation and transport packages, for the tests of every server. Only tests import it.
package observertest

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"
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

// LockObserver is an Observer that is also an operation.ILockObserver, and records the locks it is told
// about.
type LockObserver struct {
	Observer

	lockMu sync.Mutex
	locks  []Lock
}

// Lock is what a LockObserver recorded of a lock that was not acquired.
type Lock struct {
	// Type and Name are those of the operation that wanted the lock.
	Type, Name string
	Key        string
	// Held reports a call of LockHeld, and otherwise one of LockFailed, with Err.
	Held bool
	Err  error
}

// Provider returns a provider of o, for forgo.NewBootstrap.
func (o *LockObserver) Provider() func() operation.IObserver {
	return func() operation.IObserver { return o }
}

func (o *LockObserver) LockHeld(ctx context.Context, key string) {
	o.lock(ctx, Lock{Key: key, Held: true})
}

func (o *LockObserver) LockFailed(ctx context.Context, key string, err error) {
	o.lock(ctx, Lock{Key: key, Err: err})
}

func (o *LockObserver) lock(ctx context.Context, l Lock) {
	if op, ok := operation.FromCtx(ctx); ok {
		l.Type, l.Name = op.Type, op.Name
	}
	o.lockMu.Lock()
	defer o.lockMu.Unlock()
	o.locks = append(o.locks, l)
}

// Locks returns the locks recorded so far, in order.
func (o *LockObserver) Locks() []Lock {
	o.lockMu.Lock()
	defer o.lockMu.Unlock()
	return append([]Lock(nil), o.locks...)
}

// ConnObserver is an Observer that implements transport.IConnObserverFactory: it records the servers that
// ask it for a connection observer, and gives them all Conns.
type ConnObserver struct {
	Observer
	// Conns records the connections of every server.
	Conns Conns

	serverMu sync.Mutex
	servers  []transport.Server
}

// Provider returns a provider of o, for forgo.NewBootstrap.
func (o *ConnObserver) Provider() func() operation.IObserver {
	return func() operation.IObserver { return o }
}

func (o *ConnObserver) ConnObserver(server transport.Server) transport.IConnObserver {
	o.serverMu.Lock()
	defer o.serverMu.Unlock()
	o.servers = append(o.servers, server)
	return &o.Conns
}

// Servers returns the servers that asked for a connection observer, in order.
func (o *ConnObserver) Servers() []transport.Server {
	o.serverMu.Lock()
	defer o.serverMu.Unlock()
	return append([]transport.Server(nil), o.servers...)
}

// Conns is a transport.IConnStateObserver that records the states of every connection. It is safe for
// concurrent use.
type Conns struct {
	mu      sync.Mutex
	conns   []net.Conn // in the order of their first state
	states  map[net.Conn][]transport.ConnState
	changed chan struct{}
}

func (c *Conns) OnAcceptError(error) {}

func (c *Conns) OnConnState(conn net.Conn, state transport.ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states == nil {
		c.states = make(map[net.Conn][]transport.ConnState)
	}
	if _, ok := c.states[conn]; !ok {
		c.conns = append(c.conns, conn)
	}
	c.states[conn] = append(c.states[conn], state)
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

// States returns the states of every connection, in the order of their first state.
func (c *Conns) States() [][]transport.ConnState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.statesLocked()
}

func (c *Conns) statesLocked() [][]transport.ConnState {
	states := make([][]transport.ConnState, 0, len(c.conns))
	for _, conn := range c.conns {
		states = append(states, append([]transport.ConnState(nil), c.states[conn]...))
	}
	return states
}

// Conn returns the connection whose states are the ith of States.
func (c *Conns) Conn(i int) net.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conns[i]
}

// WaitClosed waits until n connections were reported closed, and returns the states of every connection.
// It fails t after 5 seconds.
func (c *Conns) WaitClosed(t testing.TB, n int) [][]transport.ConnState {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		c.mu.Lock()
		states := c.statesLocked()
		closed := 0
		for _, s := range states {
			if len(s) > 0 && s[len(s)-1] == transport.ConnClosed {
				closed++
			}
		}
		if closed >= n {
			c.mu.Unlock()
			return states
		}
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("%d connections closed, want %d: %v", closed, n, states)
		}
	}
}
