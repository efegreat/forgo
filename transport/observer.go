package transport

import (
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// IConnObserver is told about the accept errors of a server's listener, for metrics or alerts. Logging them
// is not its job: net/http logs the accept errors it retries through its ErrorLog and returns the others from
// Serve, which App logs.
//
// An observer that also implements IConnIOObserver is told about read and write errors as well. Hooks run on
// the accept loop and on every connection's goroutines: they must be safe for concurrent use and must not
// block.
type IConnObserver interface {
	// OnAcceptError is called for every failed Accept, including the temporary failures net/http retries,
	// such as running out of file descriptors. It is not called once the listener is closed.
	OnAcceptError(err error)
}

// IConnIOObserver is an IConnObserver that is also told about the read and write errors of accepted
// connections. Listener wraps connections only for such observers, because a wrapper hides the optional
// methods of the accepted connection, such as the zero-copy paths of *net.TCPConn.
//
// The hooks receive the accepted connection, not the wrapper. They are not called for the normal end of a
// connection: the end of the stream (io.EOF), a connection closed on this side (net.ErrClosed), and I/O cut
// short by a deadline that had already passed when it was set, which is how net/http cancels its own
// background reads. Each direction reports its first failure only. net/http retries a read that failed, and
// those retries fail the same way until the deadline of that direction changes.
//
// Deadlines that expire are reported. net/http uses read deadlines for ReadTimeout, ReadHeaderTimeout and
// IdleTimeout, so an idle keep-alive connection that times out reports an error matching
// os.ErrDeadlineExceeded, like a client too slow to send its request.
type IConnIOObserver interface {
	IConnObserver
	OnReadError(conn net.Conn, err error)
	OnWriteError(conn net.Conn, err error)
}

// ConnState is the state of an accepted connection, as an IConnStateObserver is told about it.
type ConnState uint8

const (
	// ConnNew is a connection just accepted.
	ConnNew ConnState = iota
	// ConnActive is a connection that is reading or serving a request. HTTP only.
	ConnActive
	// ConnIdle is a keep-alive connection between two requests. HTTP only.
	ConnIdle
	// ConnHijacked is a connection a handler took over, such as for a WebSocket. HTTP only.
	ConnHijacked
	// ConnClosed is a closed connection, a hijacked one included.
	ConnClosed
)

// String returns the name of s, such as "idle", for metric labels. A value outside the declared states
// is "unknown".
func (s ConnState) String() string {
	switch s {
	case ConnNew:
		return "new"
	case ConnActive:
		return "active"
	case ConnIdle:
		return "idle"
	case ConnHijacked:
		return "hijacked"
	case ConnClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// IConnStateObserver is an IConnObserver that is also told about the state of every connection, for gauges
// of open connections. The HTTP server reports every state, through net/http's ConnState hook, without
// wrapping connections, and reports a hijacked connection closed once its handler returns. The gRPC server
// reports only ConnNew and ConnClosed, since HTTP/2 serves many calls on one connection: its Listener wraps
// connections to see them close.
//
// conn is the same value for every state of one connection, so that an observer can keep the last state of
// each one. Every connection ends with one ConnClosed. The hook runs on the server's goroutines, so it must
// be safe for concurrent use and must not block.
type IConnStateObserver interface {
	IConnObserver
	OnConnState(conn net.Conn, state ConnState)
}

// Server is a server as its connection observer knows it, for its labels and for the states it reports.
type Server struct {
	// Name is the server_name of the server's logs: its ServerConfig.Name, or its default name, such as
	// "http", which servers of one kind share.
	Name string
	// Addr is the address the server listens on, from its configuration, which tells apart two servers of
	// one name.
	Addr string
	// ReportsActivity is set by a server that reports ConnActive, ConnIdle and ConnHijacked, as the HTTP
	// server does. A server without it reports only ConnNew and ConnClosed, as the gRPC server and a
	// Listener with ReportState do, so its connections are open from ConnNew to ConnClosed. Observers read
	// it instead of the name, which the configuration chooses.
	ReportsActivity bool
}

// IConnObserverFactory is implemented by an operation.IObserver of the bootstrap that also observes the
// connections of the HTTP and gRPC servers. One observer serves every server, and the hooks of an
// IConnObserver do not say which server they report for, so each server asks for an observer of its own.
//
// A server whose configuration has no Observer calls ConnObserver once, in New, and uses what it returns,
// nil for none. It may be an IConnIOObserver or an IConnStateObserver too.
type IConnObserverFactory interface {
	ConnObserver(server Server) IConnObserver
}

// Listener is a net.Listener that reports to Observer, and reports nothing when Observer is nil. Accepted
// connections are wrapped only when Observer implements IConnIOObserver, or IConnStateObserver with
// ReportState set; a wrapped connection has a NetConn method, like *tls.Conn, that returns the accepted one.
type Listener struct {
	net.Listener
	Observer IConnObserver
	// ReportState reports ConnNew and ConnClosed to an Observer that implements IConnStateObserver, by
	// wrapping connections to see them close. Servers that report states themselves, as the HTTP server does
	// through net/http's ConnState hook, leave it off.
	ReportState bool
}

// Accept waits for the next connection. Its failures are reported, unless the listener is closed.
func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		if l.Observer != nil && !errors.Is(err, net.ErrClosed) {
			l.Observer.OnAcceptError(err)
		}
		return nil, err
	}

	io, _ := l.Observer.(IConnIOObserver)
	var state IConnStateObserver
	if l.ReportState {
		state, _ = l.Observer.(IConnStateObserver)
	}
	if io == nil && state == nil {
		return c, nil
	}
	if state != nil {
		state.OnConnState(c, ConnNew)
	}
	return &conn{Conn: c, io: io, state: state}, nil
}

// conn is an accepted connection that reports its read and write errors to io, and its close to state,
// when they are set.
type conn struct {
	net.Conn
	io     IConnIOObserver
	state  IConnStateObserver
	read   direction
	write  direction
	closed atomic.Bool
}

func (c *conn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil && c.io != nil && c.read.failed(err) {
		c.io.OnReadError(c.Conn, err)
	}
	return n, err
}

func (c *conn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil && c.io != nil && c.write.failed(err) {
		c.io.OnWriteError(c.Conn, err)
	}
	return n, err
}

// Close closes the accepted connection, and reports the first Close as ConnClosed.
func (c *conn) Close() error {
	err := c.Conn.Close()
	if c.state != nil && c.closed.CompareAndSwap(false, true) {
		c.state.OnConnState(c.Conn, ConnClosed)
	}
	return err
}

func (c *conn) SetDeadline(t time.Time) error {
	c.read.setDeadline(t)
	c.write.setDeadline(t)
	return c.Conn.SetDeadline(t)
}

func (c *conn) SetReadDeadline(t time.Time) error {
	c.read.setDeadline(t)
	return c.Conn.SetReadDeadline(t)
}

func (c *conn) SetWriteDeadline(t time.Time) error {
	c.write.setDeadline(t)
	return c.Conn.SetWriteDeadline(t)
}

// CloseWrite shuts down the writing side when the accepted connection can, as *net.TCPConn can. net/http
// sends a FIN this way before it closes a connection whose request it did not read in full, so that the
// client gets the response instead of a reset (golang.org/issue/3595).
func (c *conn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.ErrUnsupported
}

// NetConn returns the accepted connection.
func (c *conn) NetConn() net.Conn {
	return c.Conn
}

// States of a direction. Deadlines are set from other goroutines than the I/O, so they are atomic.
const (
	directionOpen     int32 = iota // failures are reported
	directionCanceled              // the deadline had passed when it was set, so timeouts are cancellations
	directionFailed                // a failure was reported, and the next ones repeat it
)

// direction decides which errors of one direction of a connection are reported.
type direction struct {
	state atomic.Int32
}

// setDeadline records a new deadline. One that has already passed cancels pending I/O; any other one makes
// failures reportable again.
func (d *direction) setDeadline(t time.Time) {
	if !t.IsZero() && !t.After(time.Now()) {
		d.state.Store(directionCanceled)
	} else {
		d.state.Store(directionOpen)
	}
}

// failed reports whether err is a failure to report, and records that it was reported.
func (d *direction) failed(err error) bool {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		return false
	case errors.Is(err, os.ErrDeadlineExceeded):
		return d.state.CompareAndSwap(directionOpen, directionFailed)
	default:
		return d.state.Swap(directionFailed) != directionFailed
	}
}
