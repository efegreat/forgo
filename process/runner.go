package process

import (
	"context"
	"fmt"
	"sync"
)

// Runner is a struct that provides methods to execute functions either synchronously or asynchronously, while managing concurrency through a semaphore. It allows for controlled execution of tasks, ensuring that the number of concurrent tasks does not exceed the limits set by the semaphore.
type Runner struct {
	s ISemaphore
}

func NewRunner(s ISemaphore) *Runner {
	return &Runner{
		s: s,
	}
}

func (e *Runner) Do(fn func()) {
	fn()
}

func (e *Runner) DoAsync(fn func()) error {
	if err := e.s.Acquire(); err != nil {
		return err
	}

	Go(func() {
		defer e.s.Release()
		fn()
	})

	return nil
}

func (e *Runner) TryAsync(fn func()) {
	if err := e.DoAsync(fn); err != nil {
		e.Do(fn)
	}
}

// ConcurrentRunner executes functions concurrently up to a fixed limit.
//
// Run blocks until a concurrency slot becomes available or ctx is canceled.
//
// Wait must only be called after no more calls to Run will be made.
type ConcurrentRunner struct {
	s  ISemaphore
	wg sync.WaitGroup
}

func NewConcurrentRunner(s ISemaphore) *ConcurrentRunner {
	return &ConcurrentRunner{s: s}
}

// Run waits for an available concurrency slot and starts fn in a goroutine.
//
// ctx controls only waiting for a slot. Once fn has started, its lifecycle
// belongs to fn itself.
func (r *ConcurrentRunner) Run(
	ctx context.Context,
	fn func(),
) error {
	if fn == nil {
		return fmt.Errorf("runner function cannot be nil")
	}
	if err := r.s.WaitAcquire(ctx); err != nil {
		return err
	}

	r.wg.Go(func() {
		defer r.s.Release()
		fn()
	})

	return nil
}

// Wait blocks until every function successfully started by Run has finished.
//
// No calls to Run should occur after Wait begins.
func (r *ConcurrentRunner) Wait() {
	r.wg.Wait()
}
