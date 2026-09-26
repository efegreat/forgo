package http

import (
	"context"
	"crypto/tls"
	"errors"
	"forgo"
	"forgo/logger"
	"forgo/middleware"
	"forgo/operation"
	"forgo/transport"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
)

const (
	ServerNameHTTP      = "http"
	DefaultNetwork      = "tcp"
	DefaultAddr         = ":8080"
	DefaultReadTimeout  = 60 * time.Second
	DefaultWriteTimeout = 10 * time.Second
	DefaultIdleTimeout  = 60 * time.Second
)

type serverConf struct {
	network                      string
	addr                         string
	disableGeneralOptionsHandler bool
	tlsConfig                    *tls.Config
	readTimeout                  time.Duration
	readHeaderTimeout            time.Duration
	writeTimeout                 time.Duration
	idleTimeout                  time.Duration
	maxHeaderBytes               int
	tlsNextProto                 map[string]func(*http.Server, *tls.Conn, http.Handler)
	connState                    func(net.Conn, http.ConnState)
	errorLog                     *log.Logger
	connContext                  func(ctx context.Context, c net.Conn) context.Context
	http2                        *http.HTTP2Config
	protocols                    *http.Protocols
	validators                   map[string]validator.Func
	routes                       []Route
	observer                     transport.IConnObserver
	binder                       IBinder
	serializer                   SerializerFactory
	errorMapper                  *operation.ErrorMapper
}

// Server is a struct that represents an HTTP server. It contains an http.Server instance.
type Server struct {
	*forgo.Server
	s    *http.Server
	conf *serverConf
}

func NewServer(opts ...ServerOption) *Server {
	h := &Server{
		Server: &forgo.Server{},
		conf: &serverConf{
			network:      DefaultNetwork,
			addr:         DefaultAddr,
			readTimeout:  DefaultReadTimeout,
			writeTimeout: DefaultWriteTimeout,
			idleTimeout:  DefaultIdleTimeout,
		},
	}
	for _, opt := range opts {
		opt(h)
	}
	if h.Server.Tt == 0 {
		h.Server.Tt = forgo.DefaultServerTimeout
	}
	if h.conf.binder == nil {
		h.conf.binder = newDefaultBinder()
	}
	if h.conf.serializer == nil {
		h.conf.serializer = newDefaultSerializer
	}
	if h.conf.errorMapper == nil {
		h.conf.errorMapper = newDefaultErrorMapper()
	}

	return h
}

func (h *Server) Name() string {
	return ServerNameHTTP
}

func (h *Server) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	if h.conf.errorLog == nil {
		h.conf.errorLog = log.New(defaultHTTPErrorLog{l: *lg}, "", 0)
	}
	if h.conf.observer == nil {
		h.conf.observer = transport.LoggingObserver{L: *lg}
	}
	h.s = &http.Server{
		Addr:                         h.conf.addr,
		Handler:                      h.router(),
		DisableGeneralOptionsHandler: h.conf.disableGeneralOptionsHandler,
		TLSConfig:                    h.conf.tlsConfig,
		ReadTimeout:                  h.conf.readTimeout,
		ReadHeaderTimeout:            h.conf.readHeaderTimeout,
		WriteTimeout:                 h.conf.writeTimeout,
		IdleTimeout:                  h.conf.idleTimeout,
		MaxHeaderBytes:               h.conf.maxHeaderBytes,
		TLSNextProto:                 h.conf.tlsNextProto,
		ConnState:                    h.conf.connState,
		ErrorLog:                     h.conf.errorLog,
		BaseContext: func(net.Listener) context.Context {
			return context.WithoutCancel(ctx)
		},
		ConnContext: h.conf.connContext,
		HTTP2:       h.conf.http2,
		Protocols:   h.conf.protocols,
	}
	return nil
}

func (h *Server) Serve() error {
	ln, err := net.Listen(h.conf.network, h.conf.addr)
	if err != nil {
		return err
	}

	wl := &transport.Listener{
		Listener: ln,
		Observer: h.conf.observer,
	}
	if err := h.s.Serve(wl); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (h *Server) Terminate(ctx context.Context) error {
	return h.s.Shutdown(ctx)
}

func (h *Server) Kill() {}

func (h *Server) router() http.Handler {
	router := http.NewServeMux()

	// append not found route
	h.conf.routes = append(h.conf.routes, NotFoundRoute)

	for _, route := range h.conf.routes {
		router.HandleFunc(route.Method+" "+route.Path, h.createHandler(route))
	}

	return router
}

func (h *Server) createHandler(route Route) http.HandlerFunc {
	var middlewares []operation.Middleware
	if !h.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Recorder(RequestLogFields(RequestRecorderBodyLimit), ResponseLogFields()),
			middleware.Recovery(PanicRecovery(h.conf.serializer, h.conf.errorMapper)),
		)
	}

	// add server middlewares
	middlewares = append(middlewares, h.Middlewares...)

	// add route middlewares
	middlewares = append(middlewares, route.Middlewares...)

	// create final handler
	handler := route.Handler(h.conf.binder, h.conf.serializer, h.conf.errorMapper)
	handler = operation.Chain(handler, middlewares...)

	return func(w http.ResponseWriter, r *http.Request) {
		opCtx := &operation.Context{
			Context: r.Context(),
			ID:      r.Header.Get(ReqIDHeader),
			Name:    route.Name,
			Type:    OperationTypeHTTPHandler,
			State: &State{
				Request: r,
				Writer:  newResponseRecorder(w, ResponseRecorderBodyLimit),
			},
		}
		// This will be handled by net/http
		if err := handler(opCtx); errors.Is(err, http.ErrAbortHandler) {
			panic(http.ErrAbortHandler)
		}
	}
}

type ServerOption func(*Server)

func WithBaseOption(opt forgo.ServerOption) ServerOption {
	return func(s *Server) {
		opt(s.Server)
	}
}

func WithNetwork(network string) ServerOption {
	return func(s *Server) {
		s.conf.network = network
	}
}

func WithAddr(addr string) ServerOption {
	return func(h *Server) {
		h.conf.addr = addr
	}
}

func WithDisableGeneralOptionsHandler(disable bool) ServerOption {
	return func(h *Server) {
		h.conf.disableGeneralOptionsHandler = disable
	}
}

func WithTLSConfig(cfg *tls.Config) ServerOption {
	return func(h *Server) {
		h.conf.tlsConfig = cfg
	}
}

func WithReadTimeout(timeout time.Duration) ServerOption {
	return func(h *Server) {
		h.conf.readTimeout = timeout
	}
}

func WithReadHeaderTimeout(timeout time.Duration) ServerOption {
	return func(h *Server) {
		h.conf.readHeaderTimeout = timeout
	}
}

func WithWriteTimeout(timeout time.Duration) ServerOption {
	return func(h *Server) {
		h.conf.writeTimeout = timeout
	}
}

func WithIdleTimeout(timeout time.Duration) ServerOption {
	return func(h *Server) {
		h.conf.idleTimeout = timeout
	}
}

func WithMaxHeaderBytes(maxHeaderBytes int) ServerOption {
	return func(h *Server) {
		h.conf.maxHeaderBytes = maxHeaderBytes
	}
}

func WithTLSNextProto(tlsNextProto map[string]func(*http.Server, *tls.Conn, http.Handler)) ServerOption {
	return func(h *Server) {
		h.conf.tlsNextProto = tlsNextProto
	}
}

func WithConnState(connState func(net.Conn, http.ConnState)) ServerOption {
	return func(h *Server) {
		h.conf.connState = connState
	}
}

func WithErrorLog(logger *log.Logger) ServerOption {
	return func(h *Server) {
		h.conf.errorLog = logger
	}
}

func WithConnContext(connContext func(ctx context.Context, c net.Conn) context.Context) ServerOption {
	return func(h *Server) {
		h.conf.connContext = connContext
	}
}

func WithHTTP2(http2 *http.HTTP2Config) ServerOption {
	return func(h *Server) {
		h.conf.http2 = http2
	}
}

func WithProtocols(protocols *http.Protocols) ServerOption {
	return func(h *Server) {
		h.conf.protocols = protocols
	}
}

func WithValidators(validators map[string]validator.Func) ServerOption {
	return func(h *Server) {
		h.conf.validators = validators
	}
}

func WithRoutes(routes ...Route) ServerOption {
	return func(s *Server) {
		s.conf.routes = append(s.conf.routes, routes...)
	}
}

func WithObserver(observer transport.IConnObserver) ServerOption {
	return func(h *Server) {
		h.conf.observer = observer
	}
}

func WithBinder(binder IBinder) ServerOption {
	return func(h *Server) {
		h.conf.binder = binder
	}
}

func WithSerializer(s SerializerFactory) ServerOption {
	return func(h *Server) {
		h.conf.serializer = s
	}
}

func WithErrorMapper(mapper *operation.ErrorMapper) ServerOption {
	return func(h *Server) {
		h.conf.errorMapper = mapper
	}
}
