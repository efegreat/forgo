package process

type Runner struct {
	s Semaphore
}

func NewRunner(s Semaphore) *Runner {
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
