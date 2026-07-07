package process

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
