package transport

import (
	"errors"
	"net"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

// IConnObserver is an interface that defines methods for observing connection errors. It provides hooks for handling errors that occur during accepting, reading, and writing connections.
type IConnObserver interface {
	OnAcceptError(error)
	OnReadError(net.Conn, error)
	OnWriteError(net.Conn, error)
}

// LoggingObserver is the default IConnObserver that logs connection errors using zerolog. It does not log read or write errors.
type LoggingObserver struct {
	L zerolog.Logger
}

func (lo LoggingObserver) OnAcceptError(err error) {
	if !errors.Is(err, net.ErrClosed) {
		lo.L.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("accept error")
	}
}

func (lo LoggingObserver) OnReadError(conn net.Conn, err error) {}

func (lo LoggingObserver) OnWriteError(conn net.Conn, err error) {}

type Listener struct {
	net.Listener
	Observer IConnObserver
}

func (l *Listener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		l.Observer.OnAcceptError(err)
		return nil, err
	}

	return &Conn{
		Conn:     conn,
		observer: l.Observer,
	}, nil
}

type Conn struct {
	net.Conn
	observer IConnObserver
}

func (c *Conn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)

	if err != nil {
		c.observer.OnReadError(c.Conn, err)
	}

	return n, err
}

func (c *Conn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)

	if err != nil {
		c.observer.OnWriteError(c.Conn, err)
	}

	return n, err
}
