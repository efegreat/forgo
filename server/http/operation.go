package http

import (
	"context"
	"net/http"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

// argKey is the type of the Args keys the HTTP server reads. Its own type keeps them apart from the
// keys of every other package.
type argKey uint8

const (
	// ArgKeyRecorderDisabled turns the Recorder off for the routes whose Args map it to true.
	ArgKeyRecorderDisabled argKey = iota
)

// OperationTypeHTTPHandler is the type of the HTTP server's operations, logged as op_type.
const OperationTypeHTTPHandler = "http_handler"

// notFoundPattern matches every request that no route matches.
const notFoundPattern = "/"

var (
	errNotFound = operation.Error{
		Code:    "nf",
		Status:  operation.StatusNotFound,
		Message: "not found",
	}
	errTerminating = operation.Error{
		Code:     "terminating",
		Status:   operation.StatusUnavailable,
		Message:  "server is terminating",
		Severity: operation.SeverityLight, // expected while draining, logged as a warning
	}
)

// notFoundRoute is the not_found fallback: it answers the requests no route matches with errNotFound (404).
func notFoundRoute(maxBodyBytes int64) resolvedRoute {
	return resolvedRoute{
		name:    "not_found",
		pattern: notFoundPattern,
		handler: func(_ IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
			return func(ctx *operation.Context) error {
				return respondError(ctx, sf, em, errNotFound)
			}
		},
		maxBodyBytes: max(maxBodyBytes, 0),
	}
}

// recorderDisabled reports whether the Args of the operation turn the Recorder off.
func recorderDisabled(ctx *operation.Context) bool {
	return ctx.Args.GetTyped[bool](ArgKeyRecorderDisabled)
}

// Handler serves the requests of a route. It receives the request bound and validated, and returns the result
// or an error. ctx is the request's *operation.Context.
type Handler[Req, Resp any] func(
	context.Context,
	Req,
) (Resp, error)

// handlerWrapper builds the terminal handler of a route with the server's binder, serializer and error mapper.
type handlerWrapper func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler

// newHandlerWrapper builds the terminal handler of fn: it binds the request, calls fn and answers its result or
// error. A handler that wrote the response itself, through State.Writer, keeps it.
func newHandlerWrapper[Req, Resp any](fn Handler[Req, Resp]) handlerWrapper {
	return func(b IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			var req Req
			if err := b.Bind(st.Request, &req); err != nil {
				return respondError(ctx, sf, em, err)
			}

			resp, err := fn(ctx, req)
			if err != nil {
				return respondError(ctx, sf, em, err)
			}

			return respond(ctx, sf, resp)
		}
	}
}

// newEmptyHandler builds a terminal handler that answers an empty success response (200), without a request to
// bind. The result is nil, so serializers omit it, which is what the OpenAPI generator documents for it.
func newEmptyHandler() handlerWrapper {
	return func(_ IBinder, sf SerializerFactory, _ *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			return respond(ctx, sf, nil)
		}
	}
}

// newHealthHandler builds a terminal handler that answers like newEmptyHandler until ts is signaled, then with
// errTerminating (503). A nil ts is never signaled.
func newHealthHandler(ts *forgo.TermSig) handlerWrapper {
	return func(_ IBinder, sf SerializerFactory, em *operation.ErrorMapper) operation.Handler {
		return func(ctx *operation.Context) error {
			if ts != nil && ts.Signaled() {
				return respondError(ctx, sf, em, errTerminating)
			}
			return respond(ctx, sf, nil)
		}
	}
}

// State is the transport state of an HTTP operation, in operation.Context.State. Middlewares read it with
// ctx.State.(*http.State), and handlers through operation.FromCtx.
type State struct {
	// Request is the request being served.
	Request *http.Request
	// Writer writes the response. The server answers only when nothing is written yet, so a handler or
	// middleware that writes the response itself, such as a redirect or a file, keeps it.
	Writer *ResponseRecorder
}
