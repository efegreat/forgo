package http

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/inprocess"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"
	"github.com/efegreat/forgo/types"
)

// ServerNameHTTP is the default name of the HTTP server, the server_name of its logs.
const ServerNameHTTP = "http"

// server serves the routes of a Config with net/http. Config.New creates it.
type server struct {
	cfg Config
	s   *http.Server
	// observer is the operation.IObserver of the bootstrap, nil without one.
	observer operation.IObserver
	// conns is Config.Observer, or else the connection observer the bootstrap's observer gave the server.
	conns transport.IConnObserver
	// terminating is closed when Terminate starts, for State.Terminating.
	terminating   chan struct{}
	terminateOnce sync.Once
	// hijacks tracks the handlers that hijacked their connection, which http.Server does not.
	hijacks hijacks
	// base is the context requests derive from, set by Init.
	base context.Context
}

// Term returns the termination settings of the Config.
func (h *server) Term() forgo.TermConfig {
	return h.cfg.Term
}

// Init derives the context of every request from ctx, and logs net/http's own errors with ctx's logger unless
// Config.ErrorLog is set.
func (h *server) Init(ctx context.Context) error {
	h.base = context.WithoutCancel(ctx)
	h.s.BaseContext = func(net.Listener) context.Context {
		return h.base
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
	return h.ServeListener(ln)
}

// ServeListener serves on ln as Serve serves on Config.Addr, and closes ln. forgotest calls it with a
// loopback listener of its own, which it then knows the address of.
func (h *server) ServeListener(ln net.Listener) error {
	// States are reported through http.Server.ConnState, which wraps no connection.
	wl := &transport.Listener{
		Listener: ln,
		Observer: h.conns,
	}

	// With a TLS config, certificates come from TLSConfig (Certificates, GetCertificate or GetConfigForClient).
	var err error
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

// Handler returns the router of the server, with the chain of every route, to serve requests in process,
// such as with httptest.NewRecorder. forgotest calls it after Init. A request keeps the cancellation and
// values of its own context, and gets the values of the context it would derive from if the server served
// it, such as the server's logger. net/http's server does not run: none of its timeouts and limits apply,
// and a handler can hijack the connection only when the ResponseWriter supports it.
func (h *server) Handler() http.Handler {
	base, mux := h.base, h.s.Handler
	if base == nil {
		return mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r.WithContext(inprocess.Context(r.Context(), base)))
	})
}

// Terminate closes State.Terminating, stops accepting connections and waits for the requests in flight to
// finish, then for the handlers that hijacked their connection, such as for WebSockets, to return. When ctx
// ends first, it returns ctx's error and leaves the remaining connections open, for App to Kill the server.
func (h *server) Terminate(ctx context.Context) error {
	h.terminateOnce.Do(func() { close(h.terminating) })
	if err := h.s.Shutdown(ctx); err != nil {
		return fmt.Errorf("http: shutdown: %w", err)
	}
	if err := h.hijacks.wait(ctx); err != nil {
		return fmt.Errorf("http: hijacked connections: %w", err)
	}
	return nil
}

// Kill closes the listener and every connection, hijacked ones included, so the requests still in flight see
// their context canceled and their writes fail, and the handlers of hijacked connections see their reads and
// writes fail. App calls it when Terminate failed or timed out. It does not wait for the handlers.
func (h *server) Kill() {
	_ = h.s.Close()
	h.hijacks.kill()
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
			middleware.Observe(h.observer),
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
			st:     State{Request: r, terminating: h.terminating},
			writer: ResponseRecorder{ResponseWriter: w, bodyLimit: ResponseRecorderBodyLimit, hijacks: &h.hijacks},
		}
		req.st.Writer = &req.writer
		defer func() {
			if req.writer.hijacked {
				h.hijacks.done(&req.writer)
			}
		}()
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

// hijacks tracks the handlers that hijacked their connection, such as for WebSockets, until they return:
// http.Server.Shutdown forgets a connection once it is hijacked, and Close does not close it.
type hijacks struct {
	mu sync.Mutex
	// conns holds the connection of every handler that hijacked one and has not returned, nil while net/http
	// hijacks it.
	conns map[*ResponseRecorder]net.Conn
	// idle is closed when conns becomes empty, once wait waits for it.
	idle   chan struct{}
	killed bool
	// state, when set, is told when the handler of a hijacked connection returned.
	state transport.IConnStateObserver
}

// start registers the handler of w before net/http hijacks its connection. Shutdown stops waiting for the
// connection as soon as it is hijacked, and Terminate must find the handler then.
func (h *hijacks) start(w *ResponseRecorder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.conns == nil {
		h.conns = make(map[*ResponseRecorder]net.Conn)
	}
	h.conns[w] = nil
}

// hijacked records the connection the handler of w hijacked. The server may have been killed while net/http
// hijacked it, and then it is closed at once.
func (h *hijacks) hijacked(w *ResponseRecorder, c net.Conn) {
	h.mu.Lock()
	h.conns[w] = c
	killed := h.killed
	h.mu.Unlock()
	if killed {
		_ = c.Close()
	}
}

// done forgets the handler of w, which returned, or failed to hijack its connection. net/http never reports
// a hijacked connection closed, so done reports it to state, as the server stops tracking it.
func (h *hijacks) done(w *ResponseRecorder) {
	h.mu.Lock()
	c := h.conns[w]
	delete(h.conns, w)
	if len(h.conns) == 0 && h.idle != nil {
		close(h.idle)
		h.idle = nil
	}
	h.mu.Unlock()
	if c != nil && h.state != nil {
		h.state.OnConnState(c, transport.ConnClosed)
	}
}

// wait waits for the handlers that hijacked their connection to return, and returns ctx's error when ctx
// ends first. It is called once Shutdown returned, when no handler can hijack a connection anymore.
func (h *hijacks) wait(ctx context.Context) error {
	h.mu.Lock()
	if len(h.conns) == 0 {
		h.mu.Unlock()
		return nil
	}
	if h.idle == nil {
		h.idle = make(chan struct{})
	}
	idle := h.idle
	h.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// kill closes the hijacked connections, and those hijacked from now on. The handlers' reads and writes then
// fail.
func (h *hijacks) kill() {
	h.mu.Lock()
	h.killed = true
	conns := make([]net.Conn, 0, len(h.conns))
	for _, c := range h.conns {
		if c != nil {
			conns = append(conns, c)
		}
	}
	h.mu.Unlock()
	// Outside the lock: closing a TLS connection writes to it.
	for _, c := range conns {
		_ = c.Close()
	}
}

// reportConnState returns the http.Server.ConnState hook that reports every state to o, then calls next,
// the ConnState of the Config, when it is set.
func reportConnState(o transport.IConnStateObserver, next func(net.Conn, http.ConnState)) func(net.Conn, http.ConnState) {
	return func(c net.Conn, state http.ConnState) {
		o.OnConnState(c, connState(state))
		if next != nil {
			next(c, state)
		}
	}
}

// connState returns the transport.ConnState of state.
func connState(state http.ConnState) transport.ConnState {
	switch state {
	case http.StateNew:
		return transport.ConnNew
	case http.StateActive:
		return transport.ConnActive
	case http.StateIdle:
		return transport.ConnIdle
	case http.StateHijacked:
		return transport.ConnHijacked
	default:
		return transport.ConnClosed
	}
}
