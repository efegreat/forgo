package forgo

import (
	"context"
	"errors"
	"net/http"
)

// HttpServer is a struct that represents an HTTP server. It contains an http.Server instance.
type HttpServer struct {
	*Server
	s http.Server
}

func NewHttpServer(jobs []ICronJob, opts ...ServerOption) *HttpServer {
	return &HttpServer{Server: NewServer(opts...), s: http.Server{Handler: http.DefaultServeMux}}
}

func (h *HttpServer) Init(ctx context.Context) error {
	return nil
}

func (h *HttpServer) Name() ServerName {
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
