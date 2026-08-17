package operation

import (
	"context"
	"forgo/types"
)

// Context is a context that carries information about an operation, including its ID, name, type.
type Context struct {
	context.Context
	ID   string
	Name string
	Type string
	Meta func(string) string
	Args types.Map
}

// Handler is a function that takes a pointer to a Context and performs an operation. It is the core function type used to execute operations within the framework.
type Handler func(*Context) error

// Middleware is a function that takes a Handler and returns a new Handler. It is used to wrap additional functionality around the execution of an operation, such as logging, error handling, or other cross-cutting concerns.
type Middleware func(Handler) Handler

// Chain is a function that takes a Handler and a variadic number of Middleware functions, and returns a new Handler that is the result of applying the middlewares in order. It allows for the composition of multiple middlewares around a single handler, enabling modular and reusable functionality.
func Chain(handler Handler, middlewares ...Middleware) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}
