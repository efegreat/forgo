package worker

import (
	"context"
	"sync"
)

// lifecycle is what the consumer and the producer share to start their goroutines in Serve, stop them in
// Terminate and cancel their work in flight in Kill.
type lifecycle struct {
	// ctx is the context of the server's loops: the subscribers, the relay and the cleanup. Terminate
	// cancels it.
	ctx    context.Context
	cancel context.CancelFunc
	// work is the context of the work in flight: message handlers and publishes. Only Kill cancels it,
	// so that Terminate lets that work finish.
	work       context.Context
	cancelWork context.CancelFunc

	mu         sync.Mutex
	terminated bool           // keeps start from starting goroutines after Terminate
	wg         sync.WaitGroup // the goroutines that start started
}

// setup derives the contexts of the server from the context of Init.
func (l *lifecycle) setup(ctx context.Context) {
	l.ctx, l.cancel = context.WithCancel(ctx)
	l.work, l.cancelWork = context.WithCancel(context.WithoutCancel(ctx))
}

// start runs every function of fns in a goroutine of its own, unless terminate ran first. It reports
// whether it started them.
func (l *lifecycle) start(fns ...func()) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.terminated {
		return false
	}
	for _, fn := range fns {
		l.wg.Go(fn)
	}
	return true
}

// terminate cancels ctx and waits for the goroutines that start started. When done ends first, it returns
// done's error and leaves them running.
func (l *lifecycle) terminate(done context.Context) error {
	l.mu.Lock()
	l.terminated = true
	l.mu.Unlock()
	l.cancel()

	// No goroutine is added from now on, so waiting cannot race with start.
	stopped := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-done.Done():
		return done.Err()
	}
}

// kill cancels the context of the work in flight. It does not wait for it.
func (l *lifecycle) kill() {
	if l.cancelWork != nil {
		l.cancelWork()
	}
}
