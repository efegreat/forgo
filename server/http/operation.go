package http

import (
	"context"
	"errors"
	"net/http"
	"syscall"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/middleware"
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
				return serializeError(s, operation.Error{
					Code:    "nf",
					Status:  operation.StatusNotFound,
					Message: "not found",
				})
			}
		},
	}
	PanicRecovery = func(sf SerializerFactory, em *operation.ErrorMapper) func(*operation.Context, *middleware.PanicError) error {
		return func(ctx *operation.Context, p *middleware.PanicError) error {
			// cannot serialize a response at this point. This will be panicked on the outermost handler and net/http will handle it.
			if errors.Is(p, http.ErrAbortHandler) {
				return http.ErrAbortHandler
			}

			// cannot serialize a response at this point. Just return the error.
			if errors.Is(p, syscall.EPIPE) ||
				errors.Is(p, syscall.ECONNRESET) {
				return errBrokenConnection
			}

			// The mapped error decides the response. Its cause stays the panic, so that the stack is
			// logged even when a mapper or a panicked operation.Error picked the response.
			st := ctx.State.(*State)
			return serializeError(sf(ctx.ID, st.Writer), em.Map(p).Wrap(p))
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
				return serializeError(sf(ctx.ID, st.Writer), em.Map(err))
			}

			resp, err := fn(ctx, req)
			if err != nil {
				return serializeError(sf(ctx.ID, st.Writer), em.Map(err))
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
				return serializeError(s, errTerminating)
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
