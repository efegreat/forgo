package http

import (
	"context"
	"errors"
	"fmt"
	"forgo/operation"
	"net/http"
	"runtime/debug"
	"syscall"
)

const (
	OperationTypeHTTPHandler = "http_handler"
)

var (
	errBrokenConnection = errors.New("http handler connection broken")
)

var (
	NotFoundRoute = Route{
		Name:   "not_found",
		Method: "",
		Path:   "/",
		Handler: func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
			return func(ctx *operation.Context) error {
				st := ctx.State.(*State)
				s := sf(ctx.ID, st.Writer)
				return s.Error(operation.Error{
					Code:    "nf",
					Status:  operation.StatusNotFound,
					Message: "not found",
				})
			}
		},
	}
	PanicRecovery = func(sf SerializerFactory, em *operation.ErrorMapper) func(*operation.Context, any) error {
		return func(ctx *operation.Context, r any) error {
			err, ok := r.(error)

			// if it is not an error, create an error from panic and write a serialized error response
			if !ok {
				st := ctx.State.(*State)
				return sf(ctx.ID, st.Writer).Error(em.Map(fmt.Errorf("panic: %s, stack: %s", r, string(debug.Stack()))))
			}

			// cannot serialize a response at this point. This will be panicked on the outermost handler and net/http will handle it.
			if errors.Is(err, http.ErrAbortHandler) {
				return http.ErrAbortHandler
			}

			// cannot serialize a response at this point. Just return the error.
			if errors.Is(err, syscall.EPIPE) ||
				errors.Is(err, syscall.ECONNRESET) {
				return errBrokenConnection
			}

			// write a serialized error response
			st := ctx.State.(*State)
			return sf(ctx.ID, st.Writer).Error(em.Map(err))
		}
	}
)

type Route struct {
	Name        string
	Method      string
	Path        string
	Middlewares []operation.Middleware
	Handler     Handler
}

// Handler is used to wrap an operation.Handler for http requests
type Handler func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler

// NewHandler creates a Handler for http requests
func NewHandler[T, T2 any](fn func(context.Context, T) (T2, error)) Handler {
	return func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			var req T
			if err := b.Bind(st.Request, &req); err != nil {
				return sf(ctx.ID, st.Writer).Error(em.Map(err))
			}

			resp, err := fn(ctx, req)
			if err != nil {
				return sf(ctx.ID, st.Writer).Error(em.Map(err))
			}

			return sf(ctx.ID, st.Writer).Success(resp)
		}
	}
}

// NewEmptyHandler creates a Handler that returns an empty success response (200) to the client, without any processing. Particularly useful health check endpoints.
func NewEmptyHandler() Handler {
	return func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			s := sf(ctx.ID, st.Writer)
			return s.Success(struct{}{})
		}
	}
}

// State contains HTTP-specific state associated with an operation.
type State struct {
	Request *http.Request
	Writer  *responseRecorder
}
