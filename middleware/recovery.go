package middleware

import (
	"fmt"
	"runtime/debug"

	"github.com/efegreat/forgo/operation"
)

// PanicError is the error a panic recovered by Recovery becomes. Value is what was passed to panic and
// Stack is the stack of the goroutine that panicked.
//
// When Value is an error, PanicError wraps it, so errors.Is and errors.As see through the panic, for
// example to a runtime.Error or to http.ErrAbortHandler.
type PanicError struct {
	Value any
	Stack []byte
}

// Error formats the panic as Go prints an unrecovered one: the value, then the stack.
func (p *PanicError) Error() string {
	return fmt.Sprintf("panic: %v\n\n%s", p.Value, p.Stack)
}

// Unwrap returns Value when it is an error, and nil otherwise.
func (p *PanicError) Unwrap() error {
	err, _ := p.Value.(error)
	return err
}

// Recovery recovers a panic in the rest of the chain as a *PanicError, which keeps the stack. With a
// nil on, the operation fails with that error. Otherwise the operation fails with what on returns, so
// a transport can answer the panic, as the HTTP server writes an error response. A transport that
// turns the panic into another error should keep the *PanicError as its cause, so that the stack is
// logged.
func Recovery(on func(*operation.Context, *PanicError) error) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) (err error) {
			defer func() {
				r := recover()
				if r == nil {
					return
				}
				p := &PanicError{Value: r, Stack: debug.Stack()}
				if on == nil {
					err = p
					return
				}
				err = on(ctx, p)
			}()
			return next(ctx)
		}
	}
}
