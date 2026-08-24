package transport

import (
	"errors"
	"net"

	"github.com/rs/zerolog"
)

// ConnObserver is an interface that defines methods for observing connection errors. It provides hooks for handling errors that occur during accepting, reading, and writing connections.
type ConnObserver interface {
	OnAcceptError(error)
	OnReadError(net.Conn, error)
	OnWriteError(net.Conn, error)
}

// LoggingObserver is the default ConnObserver that logs connection errors using zerolog. It does not log read or write errors.
type LoggingObserver struct {
	L zerolog.Logger
}

func (lo LoggingObserver) OnAcceptError(err error) {
	if !errors.Is(err, net.ErrClosed) {
		lo.L.Error().Err(err).Msg("accept error")
	}
}

func (lo LoggingObserver) OnReadError(conn net.Conn, err error) {}

func (lo LoggingObserver) OnWriteError(conn net.Conn, err error) {}

type Listener struct {
	net.Listener
	Observer ConnObserver
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
	observer ConnObserver
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
