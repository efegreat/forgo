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

// Listener is a net.Listener that reports to Observer, and reports nothing when Observer is nil. Accepted
// connections are wrapped only when Observer implements IConnIOObserver; a wrapped connection has a NetConn
// method, like *tls.Conn, that returns the accepted one.
type Listener struct {
	net.Listener
	Observer IConnObserver
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

	if o, ok := l.Observer.(IConnIOObserver); ok {
		return &conn{Conn: c, observer: o}, nil
	}
	return c, nil
}

// conn is an accepted connection that reports its read and write errors.
type conn struct {
	net.Conn
	observer IConnIOObserver
	read     direction
	write    direction
}

func (c *conn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if err != nil && c.read.failed(err) {
		c.observer.OnReadError(c.Conn, err)
	}
	return n, err
}

func (c *conn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err != nil && c.write.failed(err) {
		c.observer.OnWriteError(c.Conn, err)
	}
	return n, err
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
