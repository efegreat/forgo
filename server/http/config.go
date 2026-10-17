package http

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"

	"github.com/go-playground/validator/v10"
)

const (
	defaultNetwork      = "tcp"
	defaultAddr         = ":8080"
	defaultReadTimeout  = 60 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultIdleTimeout  = 60 * time.Second
	defaultMaxBodyBytes = 1 << 20
)

// standardMethods are the methods whose lower-case spelling is a mistake in a route: ServeMux methods are
// case-sensitive, so "get" never matches a GET request.
var standardMethods = []string{
	http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
	http.MethodConnect, http.MethodOptions, http.MethodTrace,
}

// Config declares an HTTP server and its routes. It is a forgo.ServerFactory, so it can be declared as a
// package variable and passed to forgo.WithServers. Zero fields get the defaults of Sanitize.
type Config struct {
	forgo.ServerConfig
	// Network is the network the server listens on, such as "tcp" or "unix". Default: "tcp".
	Network string
	// Addr is the address the server listens on. Default: ":8080".
	Addr string
	// TLSConfig serves HTTPS when set. Its Certificates, GetCertificate or GetConfigForClient provide the
	// certificates.
	TLSConfig *tls.Config
	// ReadTimeout bounds reading a request, body included. Default: 60 seconds.
	ReadTimeout time.Duration
	// ReadHeaderTimeout bounds reading the request headers. Zero means ReadTimeout.
	ReadHeaderTimeout time.Duration
	// WriteTimeout bounds the time from the end of the request headers to the end of the response, so it
	// also bounds handlers. Default: 10 seconds.
	WriteTimeout time.Duration
	// IdleTimeout bounds how long a keep-alive connection waits for its next request. Default: 60 seconds.
	IdleTimeout time.Duration
	// MaxHeaderBytes bounds the request headers. Zero means http.DefaultMaxHeaderBytes (1 MiB).
	MaxHeaderBytes int
	// MaxBodyBytes bounds request bodies. Reading past it fails, and MapBody answers the failure 413 with
	// errCode "body_too_large". RouteConfig.MaxBodyBytes overrides it. Default: 1 MiB. Negative means no limit.
	MaxBodyBytes int64
	// DisableGeneralOptionsHandler, TLSNextProto, ConnState, ConnContext, HTTP2 and Protocols are set on the
	// http.Server as they are.
	DisableGeneralOptionsHandler bool
	TLSNextProto                 map[string]func(*http.Server, *tls.Conn, http.Handler)
	ConnState                    func(net.Conn, http.ConnState)
	ConnContext                  func(ctx context.Context, c net.Conn) context.Context
	HTTP2                        *http.HTTP2Config
	Protocols                    *http.Protocols
	// ErrorLog receives the errors net/http logs itself, such as failed TLS handshakes. Default: the server's
	// logger, as "http server error" lines.
	ErrorLog *log.Logger
	// Observer is told about failed accepts, and about failed reads and writes when it also implements
	// transport.IConnIOObserver.
	Observer transport.IConnObserver
	// Validators are the custom validation tags of the default binder, by tag name.
	Validators map[string]validator.Func
	// Binder binds and validates the request of every handler. Default: bingo with go-playground/validator.
	Binder IBinder
	// Serializer writes responses. Default: the JSON envelope.
	Serializer SerializerFactory
	// ErrorMapper turns the errors of binding, handlers and middlewares into the operation.Error clients get.
	// Setting it replaces the default mappers: MapBody, MapBinding and MapValidation.
	ErrorMapper *operation.ErrorMapper
	// SensitiveFields names the fields whose values are masked as [*] in logs: in request and
	// response headers, the query, path wildcards, JSON bodies at any depth and binding errors.
	// Names match case-insensitively. The credential headers Authorization, Proxy-Authorization,
	// Cookie, Set-Cookie and X-Api-Key are always masked.
	SensitiveFields []string
	// Routes are the routes the server serves.
	Routes []RouteConfig
}

// Sanitize returns a copy of c with defaults applied. The default Binder is built by New, because building it
// fails on invalid Validators.
func (c Config) Sanitize() Config {
	c.ServerConfig = c.ServerConfig.Sanitize()
	if c.Network == "" {
		c.Network = defaultNetwork
	}
	if c.Addr == "" {
		c.Addr = defaultAddr
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.Serializer == nil {
		c.Serializer = newDefaultSerializer
	}
	if c.ErrorMapper == nil {
		c.ErrorMapper = newDefaultErrorMapper()
	}
	return c
}

// Name returns ServerNameHTTP.
func (c Config) Name() string {
	return ServerNameHTTP
}

// New builds the binder, resolves the middlewares and the handler of every route, builds every route's chain
// and registers it, so that a configuration error fails at startup with the route it is about.
func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	cfg := c.Sanitize()
	if cfg.Binder == nil {
		binder, err := newDefaultBinder(cfg.Validators)
		if err != nil {
			return nil, err
		}
		cfg.Binder = binder
	}
	cfg.Binder = maskingBinder{IBinder: cfg.Binder, sensitive: newSensitiveFields(cfg.SensitiveFields)}

	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc

	s := &server{cfg: cfg, terminating: make(chan struct{})}
	mux, err := s.router(r)
	if err != nil {
		return nil, err
	}
	s.s = &http.Server{
		Addr:                         cfg.Addr,
		Handler:                      mux,
		DisableGeneralOptionsHandler: cfg.DisableGeneralOptionsHandler,
		TLSConfig:                    cfg.TLSConfig,
		ReadTimeout:                  cfg.ReadTimeout,
		ReadHeaderTimeout:            cfg.ReadHeaderTimeout,
		WriteTimeout:                 cfg.WriteTimeout,
		IdleTimeout:                  cfg.IdleTimeout,
		MaxHeaderBytes:               cfg.MaxHeaderBytes,
		TLSNextProto:                 cfg.TLSNextProto,
		ConnState:                    cfg.ConnState,
		ErrorLog:                     cfg.ErrorLog,
		ConnContext:                  cfg.ConnContext,
		HTTP2:                        cfg.HTTP2,
		Protocols:                    cfg.Protocols,
	}
	return s, nil
}

// resolveRoute validates rc and resolves its handler and middlewares.
func (c Config) resolveRoute(r forgo.Resolver, rc RouteConfig) (resolvedRoute, error) {
	if err := rc.validate(); err != nil {
		return resolvedRoute{}, err
	}
	handler, err := rc.Handler.build(r, c.Term.TermSig)
	if err != nil {
		return resolvedRoute{}, err
	}
	middlewares, err := forgo.ResolveMiddlewares(r, rc.Middlewares)
	if err != nil {
		return resolvedRoute{}, err
	}
	maxBodyBytes := rc.MaxBodyBytes
	if maxBodyBytes == 0 {
		maxBodyBytes = c.MaxBodyBytes
	}
	return resolvedRoute{
		name:         rc.Name,
		pattern:      routePattern(rc.Method, rc.Version, rc.Path),
		middlewares:  middlewares,
		handler:      handler,
		args:         maps.Clone(rc.Args), // shared by every request until one writes to its Args
		maxBodyBytes: max(maxBodyBytes, 0),
	}, nil
}

// RouteConfig declares a route: the requests it matches, and the handler and middlewares that serve them.
type RouteConfig struct {
	// Name is the operation name of the route's requests, logged as op_name. It is the operationId of the
	// route in OpenAPI documents.
	Name string
	// Description describes the route in OpenAPI documents.
	Description string
	// Method is the method the route matches, such as "GET", in upper case: methods are case-sensitive.
	// Empty matches every method.
	Method string
	// Path is a ServeMux path pattern, such as "/users/{id}".
	Path string
	// Version prefixes Path, such as "v1" for "/v1/users/{id}". Surrounding slashes are ignored.
	Version string
	// Middlewares run on the route's requests, after the base and server middlewares.
	Middlewares []forgo.MiddlewareFactory
	// Handler serves the route's requests. Required.
	Handler HandlerFactory
	// Args are static values in the Args of every request of the route, such as ArgKeyRecorderDisabled.
	Args map[any]any
	// MaxBodyBytes overrides Config.MaxBodyBytes for the route, such as for uploads. Zero means the
	// server's limit, and negative means no limit.
	MaxBodyBytes int64
	// DisableAPIDoc leaves the route out of generated API documents, such as a private admin route. The server
	// serves it as usual.
	DisableAPIDoc bool
}

// FullPath returns the path the route is served on: "/<Version><Path>", or just "<Path>" when Version is empty.
func (c RouteConfig) FullPath() string {
	return routePath(c.Version, c.Path)
}

// validate rejects a route that cannot be served as declared.
func (c RouteConfig) validate() error {
	if c.Handler.build == nil {
		return errors.New("no Handler")
	}
	if upper := strings.ToUpper(c.Method); upper != c.Method && slices.Contains(standardMethods, upper) {
		return fmt.Errorf("method %q never matches %s requests, because methods are case-sensitive: use %q",
			c.Method, upper, upper)
	}
	return nil
}

// label names the route in errors: by its name and pattern, or by its pattern when it has no name.
func (c RouteConfig) label() string {
	pattern := routePattern(c.Method, c.Version, c.Path)
	if c.Name == "" {
		return pattern
	}
	return fmt.Sprintf("%q (%s)", c.Name, pattern)
}

// routePath joins a version prefix and a route path. Surrounding slashes of the version are ignored.
func routePath(version, path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if version = strings.Trim(version, "/"); version != "" {
		path = "/" + version + path
	}
	return path
}

// routePattern returns the ServeMux pattern of a route. An empty method matches every method.
func routePattern(method, version, path string) string {
	if method == "" {
		return routePath(version, path)
	}
	return method + " " + routePath(version, path)
}

// resolvedRoute is a route whose handler and middlewares are resolved.
type resolvedRoute struct {
	name         string
	pattern      string
	middlewares  []operation.Middleware
	handler      handlerWrapper
	args         map[any]any
	maxBodyBytes int64 // zero means no limit
}

// HandlerFor serves a route with a method of a service the bootstrap provides, such as
// HandlerFor((*user.Service).Get). The receiver S is resolved once, in New, and can be an interface, as in
// HandlerFor(user.Reader.Get). Req is bound and validated from the request, and Resp is the result.
func HandlerFor[S, Req, Resp any](
	fn func(
		S,
		context.Context,
		Req,
	) (Resp, error),
) HandlerFactory {
	return HandlerFactory{
		build: func(
			r forgo.Resolver,
			_ *forgo.TermSig,
		) (handlerWrapper, error) {
			service, err := r.Resolve[S]()
			if err != nil {
				return nil, err
			}

			return newHandlerWrapper(
				func(
					ctx context.Context,
					req Req,
				) (Resp, error) {
					return fn(
						service,
						ctx,
						req,
					)
				},
			), nil
		},
		contract: HandlerContract{
			Request:  reflect.TypeFor[Req](),
			Response: reflect.TypeFor[Resp](),
		},
	}
}

// HandlerAs serves a route with the Handler that constructor returns. The constructor's parameters are
// resolved from the bootstrap, once, in New.
func HandlerAs[Req, Resp any](
	constructor any,
) HandlerFactory {
	return HandlerFactory{
		build: func(
			r forgo.Resolver,
			_ *forgo.TermSig,
		) (handlerWrapper, error) {
			fn, err := r.Call[Handler[Req, Resp]](
				constructor,
			)
			if err != nil {
				return nil, err
			}

			return newHandlerWrapper(fn), nil
		},
		contract: HandlerContract{
			Request:  reflect.TypeFor[Req](),
			Response: reflect.TypeFor[Resp](),
		},
	}
}

// EmptyHandler answers every request with an empty success response (200), without a request to bind, such
// as for liveness probes.
func EmptyHandler() HandlerFactory {
	return HandlerFactory{
		build: func(forgo.Resolver, *forgo.TermSig) (handlerWrapper, error) {
			return newEmptyHandler(), nil
		},
		contract: HandlerContract{},
	}
}

// HealthHandler answers health probes for the server it is routed on: an empty success response (200) while
// the server runs, and 503 with errCode "terminating" once the server's Term.TermSig is signaled, so a readiness
// probe fails and load balancers drain during TermSig.Wait. Without a TermSig it always succeeds.
func HealthHandler() HandlerFactory {
	return HandlerFactory{
		build: func(_ forgo.Resolver, ts *forgo.TermSig) (handlerWrapper, error) {
			return newHealthHandler(ts), nil
		},
		contract: HandlerContract{},
	}
}

// HandlerFactory builds the handler of a route when the server is created. Create one with HandlerFor,
// HandlerAs, EmptyHandler, HealthHandler, LiveConfigsHandler or SetLiveConfigHandler.
type HandlerFactory struct {
	// build resolves the handler for a server whose termination signal is the given TermSig (nil when unset).
	build    func(forgo.Resolver, *forgo.TermSig) (handlerWrapper, error)
	contract HandlerContract
}

// HandlerContract describes what a handler binds and answers, for generated API documents.
type HandlerContract struct {
	// Request is the type requests are bound to, nil when the handler binds none.
	Request reflect.Type
	// Response is the type of the result, nil when the handler answers without one.
	Response reflect.Type
}

// Contract returns what the handler binds and answers.
func (h HandlerFactory) Contract() HandlerContract {
	return h.contract
}

// MiddlewareAs is forgo.MiddlewareAs for a middleware that reads request parameters. Params is a struct tagged
// like a handler request (header, query, uri and validate tags) that describes the parameters the middleware
// reads, so the OpenAPI generator documents them on every route the middleware runs on.
//
// Params only documents: the server never binds or validates it, and the middleware still reads the request
// itself, so keep Params in line with what it reads. On a server the factory is forgo.MiddlewareAs(constructor).
func MiddlewareAs[Params any](constructor any) forgo.MiddlewareFactory {
	return withContract(MiddlewareContract{Params: reflect.TypeFor[Params]()}, forgo.MiddlewareAs(constructor))
}

// withContract returns factory, which reports contract to MiddlewareContractOf instead of constructing a
// middleware.
func withContract(contract MiddlewareContract, factory forgo.MiddlewareFactory) forgo.MiddlewareFactory {
	return func(r forgo.Resolver) (operation.Middleware, error) {
		// Only MiddlewareContractOf binds a probe. A server's bootstrap fails this lookup once, while it is built.
		if p, err := r.Resolve[*middlewareProbe](); err == nil {
			p.contract, p.found = contract, true
			return passThrough, nil
		}
		return factory(r)
	}
}

// MiddlewareContract describes what a middleware reads from requests and what it answers, for generated API
// documents. MiddlewareAs, RateLimit, RateLimitWith and JWTAuth create factories with one.
type MiddlewareContract struct {
	// Params is the Params type given to MiddlewareAs, nil for the other middlewares.
	Params reflect.Type
	// Auth is how the middleware authenticates requests, nil when it does not.
	Auth *AuthContract
	// Headers are the headers the middleware sets on every response once it ran, its rejections included.
	Headers []ResponseHeader
	// Rejections are the responses the middleware answers itself, for what a request sent or how often. Its
	// own failures, such as a limiter that cannot be reached, are left out.
	Rejections []Rejection
}

// AuthContract describes how a middleware authenticates requests: with a bearer token in the Authorization
// header, or else with the value of Cookie.
type AuthContract struct {
	// BearerFormat is the format of the token, such as "JWT".
	BearerFormat string
	// Cookie is the name of the cookie the token is read from when the request has no bearer token, empty for
	// none.
	Cookie string
	// Optional reports that requests without a token are let through.
	Optional bool
}

// ResponseHeader is a header a middleware sets on responses.
type ResponseHeader struct {
	// Name is spelled as documents show it, such as "WWW-Authenticate".
	Name        string
	Description string
	// Type is the Go type of the value, such as uint for a number of seconds.
	Type reflect.Type
}

// Rejection is a response a middleware answers itself, by failing the request with Err, which the server
// answers like any error: through its ErrorMapper and Serializer.
type Rejection struct {
	Err error
	// Headers are the headers the middleware adds to this response.
	Headers []ResponseHeader
}

// MiddlewareContractOf returns the contract of a factory created by MiddlewareAs, RateLimit, RateLimitWith or
// JWTAuth, and false for any other factory. It calls f with the resolver of a private bootstrap that provides
// nothing of the application, where such a factory reports its contract without constructing a middleware.
// Other factories fail to resolve their dependencies, or construct a middleware that is discarded. Their errors
// are ignored.
func MiddlewareContractOf(f forgo.MiddlewareFactory) (MiddlewareContract, bool) {
	if f == nil {
		return MiddlewareContract{}, false
	}
	p := new(middlewareProbe)
	boot := forgo.NewBootstrap(nil, []any{func() *middlewareProbe { return p }})
	r, err := boot.Build()
	if err != nil {
		return MiddlewareContract{}, false
	}
	defer func() { _ = boot.Close() }()
	_, _ = f(r)
	return p.contract, p.found
}

// middlewareProbe receives the contract of the MiddlewareAs factory that MiddlewareContractOf calls.
type middlewareProbe struct {
	contract MiddlewareContract
	found    bool
}

func passThrough(next operation.Handler) operation.Handler {
	return next
}
