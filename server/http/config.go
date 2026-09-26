package http

import (
	"context"
	"crypto/tls"
	"forgo"
	"forgo/operation"
	"forgo/transport"
	"log"
	"net"
	"net/http"
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
	Routes                       []Route
	Observer                     transport.IConnObserver
	Binder                       IBinder
	Serializer                   SerializerFactory
	ErrorMapper                  *operation.ErrorMapper
}

func (c Config) Sanitize() Config {
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
	if c.TermTimeout == 0 {
		c.TermTimeout = forgo.DefaultServerTimeout
	}
	if c.Binder == nil {
		c.Binder = newDefaultBinder()
	}
	if c.Serializer == nil {
		c.Serializer = newDefaultSerializer
	}
	if c.ErrorMapper == nil {
		c.ErrorMapper = newDefaultErrorMapper()
	}
	return c
}
