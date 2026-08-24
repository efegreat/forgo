package operation

import (
	"context"
)

// Context is a transport agnostic context that carries information about an operation, including its ID, name, type and state.
type Context struct {
	context.Context
	ID   string
	Name string
	Type string

	// State is transport specific state. Every transport should define their State for their particular requirements/logic.
	State any
}

// Handler is a function that takes a pointer to a Context and performs an operation. It is the core function type used to execute operations within the framework.
type Handler func(*Context) error

// Middleware is a function that takes a Handler and returns a new Handler. It is used to wrap additional functionality around the execution of an operation, such as logging, error handling, or other cross-cutting concerns.
// A Middleware and any state captured by it must be safe for concurrent use. Request-specific mutable state must live in operation.Context or transport state.
type Middleware func(Handler) Handler

// Chain is a function that takes a Handler and a variadic number of Middleware functions, and returns a new Handler that is the result of applying the middlewares in order. It allows for the composition of multiple middlewares around a single handler, enabling modular and reusable functionality.
func Chain(handler Handler, middlewares ...Middleware) Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}
