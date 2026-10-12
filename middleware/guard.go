package middleware

import "github.com/efegreat/forgo/operation"

// ErrRejected is the error of an operation that a Guard without a reject function denies.
var ErrRejected = operation.Error{
	Code:     "rejected",
	Status:   operation.StatusPermissionDenied,
	Message:  "operation not allowed",
	Severity: operation.SeverityLight,
}

// Guard runs the rest of the chain only for operations that check allows, for admission checks such as
// authorization, feature gates or idempotency. An error from check fails the operation.
//
// A denied operation gets what reject returns, or ErrRejected when reject is nil. A reject that returns
// nil ends the operation successfully without running it, as the worker's idempotency guard does for a
// message it already handled. A nil check allows every operation.
func Guard(check func(*operation.Context) (bool, error), reject func(*operation.Context) error) operation.Middleware {
	if reject == nil {
		reject = func(*operation.Context) error { return ErrRejected }
	}
	return func(next operation.Handler) operation.Handler {
		if check == nil {
			return next
		}
		return func(ctx *operation.Context) error {
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
