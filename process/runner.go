package process

import (
	"context"
	"sync"
)

// Runner starts functions in goroutines tracked like those of Go, at most as many at once as its
// semaphore allows. It never waits for a slot, so it suits code that must not block, such as a
// request handler that hands work off. It is safe for concurrent use.
type Runner struct {
	s ISemaphore
}

// NewRunner returns a Runner limited by s. Runners that share s share its limit.
func NewRunner(s ISemaphore) *Runner {
	return &Runner{
		s: s,
	}
}

// TryGo starts fn in a tracked goroutine when a slot is free, and frees the slot when fn returns.
// Otherwise it returns the semaphore's error, ErrNoSlot for a ChannelSemaphore, and does not run
// fn. fn must not be nil.
func (r *Runner) TryGo(fn func()) error {
	if err := r.s.Acquire(); err != nil {
		return err
	}

	Go(func() {
		defer r.s.Release()
		fn()
	})

	return nil
}

// GoOrRun starts fn like TryGo, and runs it in the calling goroutine when TryGo fails. A caller
// that produces work faster than the limit allows is thus slowed down instead of dropping work. fn
// must not be nil.
func (r *Runner) GoOrRun(fn func()) {
	if err := r.TryGo(fn); err != nil {
		fn()
	}
}

// Group starts functions in goroutines, at most as many at once as its semaphore allows, waiting for
// a slot when none is free. Like a sync.WaitGroup or an errgroup.Group, it is joined by its own Wait,
// and its goroutines are not tracked by WaitGoroutines, so a loop such as a broker subscriber's
// Consume can return once its work finished.
//
// Go is safe for concurrent use. Wait must only be called once no more calls to Go will be made.
type Group struct {
	s  ISemaphore
	wg sync.WaitGroup
}

// NewGroup returns a Group limited by s.
func NewGroup(s ISemaphore) *Group {
	return &Group{s: s}
}

// Go waits for a free slot, starts fn in a goroutine and frees the slot when fn returns. When ctx
// ends first, it returns the semaphore's error, which matches ErrNoSlot and ctx.Err() for a
// ChannelSemaphore, and does not run fn.
//
// ctx only bounds the wait for a slot: fn does not receive it. fn must not be nil.
func (g *Group) Go(ctx context.Context, fn func()) error {
	if err := g.s.WaitAcquire(ctx); err != nil {
		return err
	}

	g.wg.Go(func() {
		defer g.s.Release()
		fn()
	})

	return nil
}

// Wait blocks until every function started by Go has returned.
func (g *Group) Wait() {
	g.wg.Wait()
}
