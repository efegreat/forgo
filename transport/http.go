package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"forgo"
	"net/http"
	"time"
)

const ServerNameHttp = "http"

// HttpServer is a struct that represents an HTTP server. It contains an http.Server instance.
type HttpServer struct {
	*forgo.Server
	s *http.Server
}

func NewHttpServer(opts ...HttpServerOption) *HttpServer {
	h := &HttpServer{Server: &forgo.Server{}, s: &http.Server{}}
	for _, opt := range opts {
		opt(h)
	}
	if h.Server.Tt == 0 {
		h.Server.Tt = forgo.DefaultServerTimeout
	}

	return h
}

func (h *HttpServer) Init(ctx context.Context) error {
	return nil
}

func (h *HttpServer) Name() string {
	return ServerNameHttp
}

func (h *HttpServer) Serve() error {
	if err := h.s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (h *HttpServer) Terminate(ctx context.Context) error {
	return h.s.Shutdown(ctx)
}

func (h *HttpServer) Kill() {}

type HttpServerOption func(*HttpServer)

func WithBaseOption(opt forgo.ServerOption) HttpServerOption {
	return func(s *HttpServer) {
		opt(s.Server)
	}
}

func WithServer(s *http.Server) HttpServerOption {
	return func(h *HttpServer) {
		h.s = s
	}
}

/*
	s := &http.Server{
		Addr:                         "",
			Handler:                      nil,
			DisableGeneralOptionsHandler: false,
		TLSConfig:                    nil,
		ReadTimeout:                  0,
		ReadHeaderTimeout:            0,
		WriteTimeout:                 0,
		IdleTimeout:                  0,
		MaxHeaderBytes:               0,
		TLSNextProto:                 nil,
		ConnState:                    nil,
		ErrorLog:                     nil,
		BaseContext:                  nil,
		ConnContext:                  nil,
		HTTP2:                        nil,
		Protocols:                    nil,
	}
*/

func WithAddr(addr string) HttpServerOption {
	return func(h *HttpServer) {
		h.s.Addr = addr
	}
}

func WithTLSConfig(cfg *tls.Config) HttpServerOption {
	return func(h *HttpServer) {
		h.s.TLSConfig = cfg
	}
}

func WithReadTimeout(timeout time.Duration) HttpServerOption {
	return func(h *HttpServer) {
		h.s.ReadTimeout = timeout
	}
}

func WithReadHeaderTimeout(timeout time.Duration) HttpServerOption {
	return func(h *HttpServer) {
		h.s.ReadHeaderTimeout = timeout
	}
}
