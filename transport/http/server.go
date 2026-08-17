package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"forgo"
	"forgo/domain"
	"forgo/logger"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/efegreat/bingo"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
)

const (
	ServerNameHttp      = "http"
	DefaultNetwork      = "tcp"
	DefaultAddr         = ":8080"
	DefaultReadTimeout  = 60 * time.Second
	DefaultWriteTimeout = 10 * time.Second
	DefaultIdleTimeout  = 60 * time.Second
)

var httpStatusMap = map[domain.Status]int{
	domain.StatusInvalidArgument:    http.StatusBadRequest,
	domain.StatusUnauthenticated:    http.StatusUnauthorized,
	domain.StatusPermissionDenied:   http.StatusForbidden,
	domain.StatusNotFound:           http.StatusNotFound,
	domain.StatusConflict:           http.StatusConflict,
	domain.StatusAborted:            http.StatusConflict,
	domain.StatusPreconditionFailed: http.StatusPreconditionFailed,
	domain.StatusRateLimited:        http.StatusTooManyRequests,
	domain.StatusCancelled:          http.StatusRequestTimeout,
	domain.StatusTimeout:            http.StatusRequestTimeout,
	domain.StatusUnavailable:        http.StatusServiceUnavailable,
	domain.StatusInternal:           http.StatusInternalServerError,
}

// hTTPStatus converts a framework status into an HTTP status code.
// Unknown statuses default to 500.
func hTTPStatus(status domain.Status) int {
	if s, ok := httpStatusMap[status]; ok {
		return s
	}

	return http.StatusInternalServerError
}

type HttpRoute struct {
	Method  string
	Path    string
	Handler ServiceWrapper
}

// IHttpBinder is an interface that defines a method for binding an HTTP request to a Go struct. It is used to extract data from the request and populate the fields of the struct.
type IHttpBinder interface {
	Bind(*http.Request, any) error
}

type ServiceWrapper func(b IHttpBinder, sf HttpSerializerFactory) http.HandlerFunc

func NewServiceWrapper[T, T2 any](fn func(context.Context, T) (T2, error)) ServiceWrapper {
	return func(b IHttpBinder, sf HttpSerializerFactory) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var req T
			s := sf(w, r)
			if err := b.Bind(r, &req); err != nil {
				s.Error(err)
				return
			}

			resp, err := fn(r.Context(), req)
			if err != nil {
				s.Error(err)
				return
			}

			s.Success(resp)
		}
	}
}

type httpServerConf struct {
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
	routes                       []HttpRoute
	observer                     ConnObserver
	binder                       IHttpBinder
	serializer                   HttpSerializerFactory
}

// HttpServer is a struct that represents an HTTP server. It contains an http.Server instance.
type HttpServer struct {
	*forgo.Server
	s    *http.Server
	conf *httpServerConf
}

func NewHttpServer(opts ...HttpServerOption) *HttpServer {
	h := &HttpServer{
		Server: &forgo.Server{},
		conf: &httpServerConf{
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
		binder, _ := bingo.NewBinder(bingo.WithDecoder(bingo.JSONDecoder{
			DisallowUnknownFields: false,
			UseNumber:             true,
		}))
		h.conf.binder = binder
	}
	if h.conf.serializer == nil {
		h.conf.serializer = func(w http.ResponseWriter, r *http.Request) ISerializer {
			return NewDefaultHttpSerializer(w, r)
		}
	}

	return h
}

func (h *HttpServer) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	if h.conf.errorLog == nil {
		h.conf.errorLog = log.New(DefaultHttpErrorLog{l: *lg}, "", 0)
	}
	if h.conf.observer == nil {
		h.conf.observer = LoggingObserver{l: *lg}
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

func (h *HttpServer) Name() string {
	return ServerNameHttp
}

func (h *HttpServer) Serve() error {
	ln, err := net.Listen(h.conf.network, h.conf.addr)
	if err != nil {
		return err
	}

	wl := &Listener{
		Listener: ln,
		observer: h.conf.observer,
	}
	if err := h.s.Serve(wl); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (h *HttpServer) Terminate(ctx context.Context) error {
	return h.s.Shutdown(ctx)
}

func (h *HttpServer) Kill() {}

func (h *HttpServer) router() http.Handler {
	router := http.NewServeMux()

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	})

	for _, route := range h.conf.routes {
		router.HandleFunc(route.Method+" "+route.Path, route.Handler(h.conf.binder, h.conf.serializer))
	}

	return router
}

type HttpServerOption func(*HttpServer)

func WithBaseOption(opt forgo.ServerOption) HttpServerOption {
	return func(s *HttpServer) {
		opt(s.Server)
	}
}

func WithNetwork(network string) HttpServerOption {
	return func(s *HttpServer) {
		s.conf.network = network
	}
}

func WithAddr(addr string) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.addr = addr
	}
}

func WithDisableGeneralOptionsHandler(disable bool) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.disableGeneralOptionsHandler = disable
	}
}

func WithTLSConfig(cfg *tls.Config) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.tlsConfig = cfg
	}
}

func WithReadTimeout(timeout time.Duration) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.readTimeout = timeout
	}
}

func WithReadHeaderTimeout(timeout time.Duration) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.readHeaderTimeout = timeout
	}
}

func WithWriteTimeout(timeout time.Duration) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.writeTimeout = timeout
	}
}

func WithIdleTimeout(timeout time.Duration) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.idleTimeout = timeout
	}
}

func WithMaxHeaderBytes(maxHeaderBytes int) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.maxHeaderBytes = maxHeaderBytes
	}
}

func WithTLSNextProto(tlsNextProto map[string]func(*http.Server, *tls.Conn, http.Handler)) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.tlsNextProto = tlsNextProto
	}
}

func WithConnState(connState func(net.Conn, http.ConnState)) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.connState = connState
	}
}

func WithErrorLog(logger *log.Logger) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.errorLog = logger
	}
}

func WithConnContext(connContext func(ctx context.Context, c net.Conn) context.Context) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.connContext = connContext
	}
}

func WithHTTP2(http2 *http.HTTP2Config) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.http2 = http2
	}
}

func WithProtocols(protocols *http.Protocols) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.protocols = protocols
	}
}

func WithValidators(validators map[string]validator.Func) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.validators = validators
	}
}

func WithRoutes(routes ...HttpRoute) HttpServerOption {
	return func(s *HttpServer) {
		s.conf.routes = append(s.conf.routes, routes...)
	}
}

func WithObserver(observer ConnObserver) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.observer = observer
	}
}

func WithBinder(binder IHttpBinder) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.binder = binder
	}
}

func WithSerializer(s HttpSerializerFactory) HttpServerOption {
	return func(h *HttpServer) {
		h.conf.serializer = s
	}
}

type DefaultHttpErrorLog struct {
	l zerolog.Logger
}

func (w DefaultHttpErrorLog) Write(p []byte) (int, error) {
	l := len(p)
	if l > 0 && p[l-1] == '\n' {
		p = p[:l-1]
	}
	w.l.Error().Err(errors.New(string(p))).Msg("http server error")
	return l, nil
}
