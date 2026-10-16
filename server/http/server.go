package http

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"
	"github.com/efegreat/forgo/types"
)

// ServerNameHTTP is the server_name of the HTTP server's logs.
const ServerNameHTTP = "http"

// server serves the routes of a Config with net/http. Config.New creates it.
type server struct {
	cfg Config
	s   *http.Server
}

// Term returns the termination settings of the Config.
func (h *server) Term() forgo.TermConfig {
	return h.cfg.Term
}

// Name returns ServerNameHTTP.
func (h *server) Name() string {
	return ServerNameHTTP
}

// Init derives the context of every request from ctx, and logs net/http's own errors with ctx's logger unless
// Config.ErrorLog is set.
func (h *server) Init(ctx context.Context) error {
	h.s.BaseContext = func(net.Listener) context.Context {
		return context.WithoutCancel(ctx)
	}
	if h.s.ErrorLog == nil {
		h.s.ErrorLog = log.New(defaultHTTPErrorLog{l: *logger.FromCtx(ctx)}, "", 0)
	}
	return nil
}

// Serve listens on Config.Addr and serves until Terminate or Kill, then returns nil. It serves HTTPS when
// Config.TLSConfig is set. After Terminate or Kill, it returns at once.
func (h *server) Serve() error {
	ln, err := net.Listen(h.cfg.Network, h.cfg.Addr)
	if err != nil {
		return err
	}

	wl := &transport.Listener{
		Listener: ln,
		Observer: h.cfg.Observer,
	}

	// With a TLS config, certificates come from TLSConfig (Certificates, GetCertificate or GetConfigForClient).
	if h.cfg.TLSConfig != nil {
		err = h.s.ServeTLS(wl, "", "")
	} else {
		err = h.s.Serve(wl)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

// Terminate stops accepting connections and waits for the requests in flight to finish. When ctx ends first,
// it returns ctx's error and leaves the remaining connections open, for App to Kill the server. It does not
// wait for hijacked connections, such as WebSockets.
func (h *server) Terminate(ctx context.Context) error {
	if err := h.s.Shutdown(ctx); err != nil {
		return fmt.Errorf("http: shutdown: %w", err)
	}
	return nil
}

// Kill closes the listener and every connection, so the requests still in flight see their context canceled
// and their writes fail. App calls it when Terminate failed or timed out. It does not wait for the handlers.
func (h *server) Kill() {
	_ = h.s.Close()
}

// router resolves the routes, builds their chains and registers them, then registers the not_found fallback
// unless a route already matches every path. Errors name their route. ServeMux panics on an invalid pattern or
// one that conflicts with another, at startup.
func (h *server) router(r forgo.Resolver) (*http.ServeMux, error) {
	mux := http.NewServeMux()
	fallback := true
	for _, rc := range h.cfg.Routes {
		route, err := h.cfg.resolveRoute(r, rc)
		if err != nil {
			return nil, fmt.Errorf("http: route %s: %w", rc.label(), err)
		}
		mux.HandleFunc(route.pattern, h.createHandler(route))
		fallback = fallback && route.pattern != notFoundPattern
	}

	if fallback {
		mux.HandleFunc(notFoundPattern, h.createHandler(notFoundRoute(h.cfg.MaxBodyBytes)))
	}
	return mux, nil
}

// request holds the values the server allocates for each request, in one allocation: its operation, the
// operation's State and Args, and the response recorder.
type request struct {
	op     operation.Context
	st     State
	args   types.Map[any, any]
	writer ResponseRecorder
}

// createHandler builds the chain of a route once, and returns the http.HandlerFunc that runs it for each
// request.
func (h *server) createHandler(route resolvedRoute) http.HandlerFunc {
	var middlewares []operation.Middleware
	if !h.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Recorder(
				recorderDisabled,
				RequestLogFields(RequestRecorderBodyLimit, RequestRecorderHeaderLimit, h.cfg.SensitiveFields),
				ResponseLogFields(ResponseRecorderHeaderLimit, h.cfg.SensitiveFields),
			),
			// Answers the *middleware.PanicError of Recovery like any other error.
			ErrorResponder(h.cfg.Serializer, h.cfg.ErrorMapper),
			middleware.Recovery(nil),
		)
	}

	// add server middlewares
	middlewares = append(middlewares, h.cfg.ResolvedMiddlewares()...)

	// add route middlewares
	middlewares = append(middlewares, route.middlewares...)

	// create final handler
	handler := route.handler(h.cfg.Binder, h.cfg.Serializer, h.cfg.ErrorMapper)
	handler = operation.Chain(handler, middlewares...)

	return func(w http.ResponseWriter, r *http.Request) {
		if route.maxBodyBytes > 0 && r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, route.maxBodyBytes)
		}
		req := &request{
			st:     State{Request: r},
			writer: ResponseRecorder{ResponseWriter: w, bodyLimit: ResponseRecorderBodyLimit},
		}
		req.st.Writer = &req.writer
		req.args.ResetCopyOnWrite(route.args)
		req.op = operation.Context{
			Context: r.Context(),
			ID:      r.Header.Get(ReqIDHeader),
			Name:    route.name,
			Type:    OperationTypeHTTPHandler,
			Args:    &req.args,
			State:   &req.st,
		}
		opCtx := &req.op
		err := handler(opCtx)
		// net/http aborts the response and closes the connection.
		if errors.Is(err, http.ErrAbortHandler) {
			panic(http.ErrAbortHandler)
		}
		// Without base middlewares nothing else answers a failed operation that wrote no response.
		_ = respondError(opCtx, h.cfg.Serializer, h.cfg.ErrorMapper, err)
	}
}
