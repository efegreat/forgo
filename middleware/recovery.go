package middleware

import (
	"fmt"
	"runtime/debug"

	"github.com/efegreat/forgo/operation"
)

// Recovery is a middleware that recovers from panics, runs the recovery hook and returns the error.
func Recovery(on func(*operation.Context, any) error) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) (err error) {
			defer func() {
				if r := recover(); r != nil {
					if on != nil {
						err = on(ctx, r)
						return
					}
					err = fmt.Errorf("panic: %s, stack: %s", r, string(debug.Stack()))
				}
			}()
			return next(ctx)
		}
	}
}
