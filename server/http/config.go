package http

import (
	"context"
	"crypto/tls"
	"forgo"
	"forgo/operation"
	"forgo/transport"
	"log"
	"maps"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

const (
	defaultNetwork      = "tcp"
	defaultAddr         = ":8080"
	defaultReadTimeout  = 60 * time.Second
	defaultWriteTimeout = 10 * time.Second
	defaultIdleTimeout  = 60 * time.Second
)

type Config struct {
	forgo.ServerConfig
	Network                      string
	Addr                         string
	DisableGeneralOptionsHandler bool
	TlsConfig                    *tls.Config
	ReadTimeout                  time.Duration
	ReadHeaderTimeout            time.Duration
	WriteTimeout                 time.Duration
	IdleTimeout                  time.Duration
	MaxHeaderBytes               int
	TlsNextProto                 map[string]func(*http.Server, *tls.Conn, http.Handler)
	ConnState                    func(net.Conn, http.ConnState)
	ErrorLog                     *log.Logger
	ConnContext                  func(ctx context.Context, c net.Conn) context.Context
	Http2                        *http.HTTP2Config
	Protocols                    *http.Protocols
	Validators                   map[string]validator.Func
	Routes                       []RouteConfig
	Observer                     transport.IConnObserver
	Binder                       IBinder
	Serializer                   SerializerFactory
	ErrorMapper                  *operation.ErrorMapper
	routes                       []resolvedRoute
}

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
	if c.Binder == nil {
		c.Binder = newDefaultBinder(c.Validators)
	}
	if c.Serializer == nil {
		c.Serializer = newDefaultSerializer
	}
	if c.ErrorMapper == nil {
		c.ErrorMapper = newDefaultErrorMapper()
	}
	return c
}

func (c Config) Name() string {
	return ServerNameHTTP
}

func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	cfg := c.Sanitize()
	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc
	cfg, err = cfg.resolveRoutes(r)
	if err != nil {
		return nil, err
	}

	return &Server{cfg: cfg, s: nil}, nil
}

func (c Config) resolveRoutes(r forgo.Resolver) (Config, error) {
	for _, rc := range c.Routes {
		handler, err := rc.Handler.build(r)
		if err != nil {
			return Config{}, err
		}
		middlewares, err := forgo.ResolveMiddlewares(r, rc.Middlewares)
		if err != nil {
			return Config{}, err
		}
		c.routes = append(c.routes, resolvedRoute{
			Name:        rc.Name,
			Method:      rc.Method,
			Path:        rc.Path,
			Version:     rc.Version,
			Middlewares: middlewares,
			Handler:     handler,
			Args:        maps.Clone(rc.Args), // shared by every request until one writes to its Args
		})
	}
	return c, nil
}

type RouteConfig struct {
	Name        string
	Description string
	Method      string
	Path        string
	Version     string
	Middlewares []forgo.MiddlewareFactory
	Handler     HandlerFactory
	Args        map[any]any
}

// FullPath returns the path the route is served on: "/<Version><Path>", or just "<Path>" when Version is empty.
func (c RouteConfig) FullPath() string {
	return routePath(c.Version, c.Path)
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

type resolvedRoute struct {
	Name        string
	Method      string
	Path        string
	Version     string
	Middlewares []operation.Middleware
	Handler     handlerWrapper
	Args        map[any]any
}

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

func HandlerAs[Req, Resp any](
	constructor any,
) HandlerFactory {
	return HandlerFactory{
		build: func(
			r forgo.Resolver,
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

func EmptyHandler() HandlerFactory {
	return HandlerFactory{
		build: func(forgo.Resolver) (handlerWrapper, error) {
			return newEmptyHandler(), nil
		},
		contract: HandlerContract{},
	}
}

type HandlerFactory struct {
	build    func(forgo.Resolver) (handlerWrapper, error)
	contract HandlerContract
}

type HandlerContract struct {
	Request  reflect.Type
	Response reflect.Type
}

func (h HandlerFactory) Contract() HandlerContract {
	return h.contract
}
