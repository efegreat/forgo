package middleware

import "forgo/operation"

// Guard checks whether an operation may proceed.
// It can be used for authorization, rate limiting, idempotency,
// validation, feature gating, and other admission checks.
func Guard(check func(*operation.Context) (bool, error), reject func(*operation.Context) error) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if check == nil {
				return next(ctx)
			}

			allowed, err := check(ctx)
			if err != nil {
				return err
			}
			if !allowed {
				return reject(ctx)
			}

			return next(ctx)
		}
	}
}
