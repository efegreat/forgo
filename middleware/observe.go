package middleware

import (
	"time"

	"github.com/efegreat/forgo/operation"
)

// Observe tells o about every operation it wraps: Start before it runs, then End with its elapsed time and
// its error, along with operation.ErrorOf of that error. A nil o observes nothing and adds nothing to the
// chain, so servers add Observe to their base chain whether or not an observer is provided.
//
// Observe does not recover panics, which only Recovery does: put it outside Recovery, as the base chains do,
// so that a panic reaches it as Recovery's *PanicError. A panic that reaches Observe skips End.
func Observe(o operation.IObserver) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		if o == nil {
			return next
		}
		return func(ctx *operation.Context) error {
			o.Start(ctx)
			start := time.Now()
			err := next(ctx)
			obs := operation.Observation{Elapsed: time.Since(start), Err: err}
			if err != nil {
				obs.Error = operation.ErrorOf(err)
			}
			o.End(ctx, obs)
			return err
		}
	}
}
