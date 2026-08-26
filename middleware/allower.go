package middleware

import "forgo/operation"

// Allower is a middleware that checks if an operation is allowed before proceeding. Allower can be used for a wide range of logics like rate limiting, auth, idempotency...
func Allower(check func(*operation.Context) (bool, error), reject func(*operation.Context) error) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if check == nil {
				return next(ctx)
			}

			allow, err := check(ctx)
			if err != nil {
				return err
			}
			if !allow {
				return reject(ctx)
			}

			return next(ctx)
		}
	}
}
