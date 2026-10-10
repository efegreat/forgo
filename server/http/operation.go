package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"syscall"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

var (
	ArgKeyRecorderDisabled = operation.ArgKey{}
)

const (
	OperationTypeHTTPHandler = "http_handler"
)

var (
	errBrokenConnection = errors.New("http handler connection broken")
	errTerminating      = operation.Error{
		Code:     "terminating",
		Status:   operation.StatusUnavailable,
		Message:  "server is terminating",
		Severity: operation.SeverityLight, // expected while draining, logged as a warning
	}
)

var (
	NotFoundRoute = resolvedRoute{
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
	RecorderDisabled = func() func(ctx *operation.Context) bool {
		return func(ctx *operation.Context) bool {
			return ctx.Args.GetTyped[bool](ArgKeyRecorderDisabled)
		}
	}
)

// Handler is a function that handles an HTTP request and returns a response or an error.
type Handler[Req, Resp any] func(
	context.Context,
	Req,
) (Resp, error)

// handlerWrapper is used to wrap an operation.Handler for http requests
type handlerWrapper func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler

// newHandlerWrapper creates a handlerWrapper for http requests
func newHandlerWrapper[T, T2 any](fn Handler[T, T2]) handlerWrapper {
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

// newEmptyHandler creates a handlerWrapper that returns an empty success response (200) to the client, without any processing. Particularly useful health check endpoints.
// The result is nil, so serializers omit it; this is the contract the OpenAPI generator documents for empty handlers.
func newEmptyHandler() handlerWrapper {
	return func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			s := sf(ctx.ID, st.Writer)
			return s.Success(nil)
		}
	}
}

// newHealthHandler creates a handlerWrapper that answers like newEmptyHandler until ts is signaled, then with
// errTerminating (503). A nil ts is never signaled.
func newHealthHandler(ts *forgo.TermSig) handlerWrapper {
	return func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			s := sf(ctx.ID, st.Writer)
			if ts != nil && ts.Signaled() {
				return s.Error(errTerminating)
			}
			return s.Success(nil)
		}
	}
}

// State contains HTTP-specific state associated with an operation.
type State struct {
	Request *http.Request
	Writer  *responseRecorder
}
