package http

import (
	"context"
	"errors"
	"forgo"
	"forgo/logger"
	"forgo/middleware"
	"forgo/operation"
	"forgo/transport"
	"forgo/types"
	"log"
	"net"
	"net/http"
	"strings"
)

const ServerNameHTTP = "http"

// Server is a struct that represents an HTTP server. It contains an http.Server instance.
type Server struct {
	cfg Config
	s   *http.Server
}

func (h *Server) Term() forgo.TermConfig {
	return h.cfg.Term
}

func (h *Server) Name() string {
	return ServerNameHTTP
}

func (h *Server) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	if h.cfg.ErrorLog == nil {
		h.cfg.ErrorLog = log.New(defaultHTTPErrorLog{l: *lg}, "", 0)
	}
	if h.cfg.Observer == nil {
		h.cfg.Observer = transport.LoggingObserver{L: *lg}
	}
	h.s = &http.Server{
		Addr:                         h.cfg.Addr,
		Handler:                      h.router(),
		DisableGeneralOptionsHandler: h.cfg.DisableGeneralOptionsHandler,
		TLSConfig:                    h.cfg.TlsConfig,
		ReadTimeout:                  h.cfg.ReadTimeout,
		ReadHeaderTimeout:            h.cfg.ReadHeaderTimeout,
		WriteTimeout:                 h.cfg.WriteTimeout,
		IdleTimeout:                  h.cfg.IdleTimeout,
		MaxHeaderBytes:               h.cfg.MaxHeaderBytes,
		TLSNextProto:                 h.cfg.TlsNextProto,
		ConnState:                    h.cfg.ConnState,
		ErrorLog:                     h.cfg.ErrorLog,
		BaseContext: func(net.Listener) context.Context {
			return context.WithoutCancel(ctx)
		},
		ConnContext: h.cfg.ConnContext,
		HTTP2:       h.cfg.Http2,
		Protocols:   h.cfg.Protocols,
	}
	return nil
}

func (h *Server) Serve() error {
	ln, err := net.Listen(h.cfg.Network, h.cfg.Addr)
	if err != nil {
		return err
	}

	wl := &transport.Listener{
		Listener: ln,
		Observer: h.cfg.Observer,
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
	h.cfg.routes = append(h.cfg.routes, NotFoundRoute)

	for _, route := range h.cfg.routes {
		if !strings.HasPrefix(route.Version, "/") {
			route.Version = "/" + route.Version
		}
		if !strings.HasPrefix(route.Path, "/") {
			route.Path = "/" + route.Path
		}
		router.HandleFunc(route.Method+" "+route.Version+route.Path, h.createHandler(route))
	}

	return router
}

func (h *Server) createHandler(route resolvedRoute) http.HandlerFunc {
	var middlewares []operation.Middleware
	if !h.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Recorder(RecorderDisabled(), RequestLogFields(RequestRecorderBodyLimit, RequestRecorderHeaderLimit), ResponseLogFields(ResponseRecorderHeaderLimit)),
			middleware.Recovery(PanicRecovery(h.cfg.Serializer, h.cfg.ErrorMapper)),
		)
	}

	// add server middlewares
	middlewares = append(middlewares, h.cfg.ResolvedMiddlewares()...)

	// add route middlewares
	middlewares = append(middlewares, route.Middlewares...)

	// create final handler
	handler := route.Handler(h.cfg.Binder, h.cfg.Serializer, h.cfg.ErrorMapper)
	handler = operation.Chain(handler, middlewares...)

	return func(w http.ResponseWriter, r *http.Request) {
		opCtx := &operation.Context{
			Context: r.Context(),
			ID:      r.Header.Get(ReqIDHeader),
			Name:    route.Name,
			Type:    OperationTypeHTTPHandler,
			Args:    types.NewMapWithData(route.Args),
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
