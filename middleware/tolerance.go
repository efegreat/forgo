package middleware

import (
	"context"
	"time"

	"github.com/efegreat/forgo/operation"
)

// Tolerance retries the rest of the chain as conf describes (see operation.Tolerance), waiting
// conf.RetryDelay between attempts. The operation fails with the error of its last attempt.
//
// Retries stop when the operation's context ends, or when conf.RetryTimeout runs out. Each attempt runs
// on a context bounded by that timeout, and the wait for the next attempt ends early. Every attempt
// starts from the same ctx.Context, which Tolerance restores before it returns.
func Tolerance(conf operation.Tolerance) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		if !conf.Retry {
			return next
		}
		return func(ctx *operation.Context) error {
			parent := ctx.Context
			defer func() { ctx.Context = parent }()

			runCtx := parent
			if conf.RetryTimeout > 0 {
				var cancel context.CancelFunc
				runCtx, cancel = context.WithTimeout(parent, conf.RetryTimeout)
				defer cancel()
			}

			err := retry(ctx, runCtx, conf, next)
			ctx.Context = parent
			if err != nil {
				if conf.OnFailure != nil {
					conf.OnFailure(ctx, err)
				}
				return err
			}
			if conf.OnSuccess != nil {
				conf.OnSuccess(ctx)
			}
			return nil
		}
	}
}

// retry runs next on runCtx until it succeeds or conf allows no more attempts, and returns the error of
// the last attempt.
func retry(ctx *operation.Context, runCtx context.Context, conf operation.Tolerance, next operation.Handler) error {
	var err error
	for attempt := 1; ; attempt++ {
		ctx.Context = runCtx
		if attempt > 1 && conf.OnRetry != nil {
			conf.OnRetry(ctx, attempt-1, err)
		}
		if err = next(ctx); err == nil {
			return nil
		}
		if attempt == conf.RetryAttempts || (conf.RetryIf != nil && !conf.RetryIf(err)) {
			return err
		}
		if !wait(runCtx, conf.RetryDelay) {
			return err
		}
	}
}

// wait waits for d and reports whether ctx is still alive, returning false as soon as ctx ends.
func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}
