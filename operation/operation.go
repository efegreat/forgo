package operation

import (
	"context"

	"github.com/efegreat/forgo/safemap"
)

// Context is the context of one operation. It is a context.Context, so handlers and the services
// they call receive it as their ctx, and FromCtx finds it again in any context derived from it.
//
// Middlewares may replace Context, ID and the other plain fields before they call the next handler,
// and Tolerance resets Context before every attempt. Goroutines the operation starts must therefore
// only read them, and write to Args, which is safe for concurrent use.
type Context struct {
	context.Context
	// ID identifies the operation in logs and responses. It is empty until middleware.ID sets it,
	// except on HTTP, where it starts as the request's X-Request-ID header.
	ID string
	// Name is the route, job, message or command name.
	Name string
	// Type is the transport's operation type, such as "http_handler".
	Type string

	// Args holds per-operation values, which Value looks up first. Transports fill it with the static
	// Args of a route or message handler. Use keys of your own unexported types, as for
	// context.WithValue.
	Args *safemap.Map[any, any]

	// State is transport specific state. Every transport defines its own State type.
	State any
}

// contextKey is the key under which a Context returns itself from Value, so that FromCtx finds it in
// the contexts derived from it.
type contextKey struct{}

// Value returns the value of key in Args, and otherwise the value of key in the embedded context.
// A nil Args is skipped.
func (c *Context) Value(key any) any {
	if _, ok := key.(contextKey); ok {
		return c
	}
	if c.Args != nil {
		if v, ok := c.Args.Get(key); ok {
			return v
		}
	}

	return c.Context.Value(key)
}

// FromCtx returns the operation ctx belongs to: ctx itself when it is a *Context, otherwise the
// innermost *Context ctx was derived from, as through context.WithTimeout. It returns false when ctx
// belongs to no operation.
func FromCtx(ctx context.Context) (*Context, bool) {
	if op, ok := ctx.(*Context); ok {
		return op, true
	}
	if ctx == nil {
		return nil, false
	}
	op, ok := ctx.Value(contextKey{}).(*Context)
	return op, ok
}

// Handler runs an operation. It is the terminal handler of a chain, or a Middleware's next.
type Handler func(*Context) error

// Middleware wraps a Handler with behavior every operation shares, such as logging or locking. It
// fails an operation by returning an error without calling next.
//
// A Middleware and any state captured by it must be safe for concurrent use. Request-specific mutable
// state must live in the Context, its Args or the transport state.
type Middleware func(Handler) Handler

// Chain wraps handler with middlewares, the first one outermost: Chain(h, a, b) runs a, then b, then h.
// Servers build their chains once, before serving, so the middlewares' constructors never run per
// operation.
func Chain(handler Handler, middlewares ...Middleware) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}
