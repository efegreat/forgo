package middleware

import (
	"forgo/operation"

	"github.com/avast/retry-go/v4"
)

// Tolerance is a middleware that provides retry and error handling capabilities for operations. It takes a configuration struct that defines the retry behavior, including the number of attempts, delay between retries, and conditions for retrying. It also allows for custom functions to be executed on success, failure, and during retries.
func Tolerance(conf operation.Tolerance) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if !conf.Retry {
				return next(ctx)
			}

			err := retry.Do(
				func() error {
					return next(ctx)
				},
				retry.Attempts(conf.RetryAttempts),
				retry.OnRetry(func(attempt uint, err error) {
					or := conf.OnRetry
					if or == nil {
						return
					}
					or(ctx, attempt, err)
				}),
				retry.Delay(conf.RetryDelay),
				retry.DelayType(retry.FixedDelay),
				retry.RetryIf(func(err error) bool {
					ri := conf.RetryIf
					if ri == nil {
						// If RetryIf is not defined, we will retry on all errors.
						return true
					}
					return ri(err)
				}),
			)

			if err != nil {
				of := conf.OnFailure
				if of != nil {
					of(ctx, err)
				}
				return err
			}

			if os := conf.OnSuccess; os != nil {
				os(ctx)
			}
			return nil
		}
	}
}
