package middleware

import (
	"fmt"
	"forgo/operation"
	"runtime/debug"
)

// Recovery is a middleware that recovers from panics and sets the error in the operation context.
func Recovery() operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) (err error) {
			defer func() {
				if r := recover(); r != nil {
					pErr := fmt.Errorf("panic recovered: %s, stack: %s", r, string(debug.Stack()))
					err = operation.Error{Severity: operation.SeverityCritical, Err: pErr}
				}
			}()
			return next(ctx)
		}
	}
}
