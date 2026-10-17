package transport_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/efegreat/forgo/transport"
)

// recorder is an IConnIOObserver that keeps what it is told.
type recorder struct {
	mu                  sync.Mutex
	accept, read, write []error
	conns               []net.Conn
}

func (r *recorder) OnAcceptError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.accept = append(r.accept, err)
}

func (r *recorder) OnReadError(c net.Conn, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.read = append(r.read, err)
	r.conns = append(r.conns, c)
}

func (r *recorder) OnWriteError(c net.Conn, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.write = append(r.write, err)
	r.conns = append(r.conns, c)
}

func (r *recorder) all() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Concat(r.accept, r.read, r.write)
}

// acceptObserver is told about accept errors only.
type acceptObserver func(error)

func (f acceptObserver) OnAcceptError(err error) { f(err) }

// stubListener hands out conns, then fails with err.
type stubListener struct {
	net.Listener
	conns []net.Conn
	err   error
}

func (l *stubListener) Accept() (net.Conn, error) {
	if len(l.conns) == 0 {
		return nil, l.err
	}
	c := l.conns[0]
	l.conns = l.conns[1:]
	return c, nil
}

// stubConn fails every read and write with err. It has no CloseWrite.
type stubConn struct {
	net.Conn
	err error
}

func (c *stubConn) Read([]byte) (int, error)         { return 0, c.err }
func (c *stubConn) Write([]byte) (int, error)        { return 0, c.err }
func (c *stubConn) SetDeadline(time.Time) error      { return nil }
func (c *stubConn) SetReadDeadline(time.Time) error  { return nil }
func (c *stubConn) SetWriteDeadline(time.Time) error { return nil }

func accept(t *testing.T, l net.Listener) net.Conn {
	t.Helper()
	c, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAcceptErrors(t *testing.T) {
	failure := &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	closed := &net.OpError{Op: "accept", Net: "tcp", Err: net.ErrClosed}
	for _, tc := range []struct {
		name     string
		err      error
		reported bool
	}{
		{"failure", failure, true},
		{"closed listener", closed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []error
			l := &transport.Listener{
				Listener: &stubListener{err: tc.err},
				Observer: acceptObserver(func(err error) { got = append(got, err) }),
			}
			if c, err := l.Accept(); c != nil || err != tc.err {
				t.Fatalf("Accept = %v, %v; want nil, %v", c, err, tc.err)
			}
			if tc.reported != (len(got) == 1 && got[0] == tc.err) || len(got) > 1 {
				t.Fatalf("reported %v, want reported=%v", got, tc.reported)
			}
		})
	}
}

// TestConnWrapping checks that only an IConnIOObserver gets wrapped connections, and that a nil
// observer is a working zero value.
func TestConnWrapping(t *testing.T) {
	for _, tc := range []struct {
		name     string
		observer transport.IConnObserver
		wrapped  bool
	}{
		{"nil observer", nil, false},
		{"accept observer", acceptObserver(func(error) {}), false},
		{"io observer", &recorder{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accepted := &stubConn{}
			l := &transport.Listener{
				Listener: &stubListener{conns: []net.Conn{accepted}, err: syscall.EMFILE},
				Observer: tc.observer,
			}
			c := accept(t, l)
			if _, err := l.Accept(); !errors.Is(err, syscall.EMFILE) {
				t.Fatalf("second Accept: %v", err)
			}

			if !tc.wrapped {
				if c != accepted {
					t.Fatalf("got %T, want the accepted connection", c)
				}
				return
			}
			nc, ok := c.(interface{ NetConn() net.Conn })
			if !ok || nc.NetConn() != accepted {
				t.Fatalf("wrapped %T does not return the accepted connection", c)
			}
		})
	}
}

func TestConnErrors(t *testing.T) {
	reset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	timeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	closed := &net.OpError{Op: "read", Net: "tcp", Err: net.ErrClosed}
	past, future := time.Unix(1, 0), time.Now().Add(time.Hour)

	directions := []struct {
		name        string
		setDeadline func(net.Conn, time.Time) error
		do          func(net.Conn) error
		reported    func(*recorder) []error
	}{
		{"read", net.Conn.SetReadDeadline,
			func(c net.Conn) error { _, err := c.Read(nil); return err },
			func(r *recorder) []error { return r.read }},
		{"write", net.Conn.SetWriteDeadline,
			func(c net.Conn) error { _, err := c.Write(nil); return err },
			func(r *recorder) []error { return r.write }},
		{"read with SetDeadline", net.Conn.SetDeadline,
			func(c net.Conn) error { _, err := c.Read(nil); return err },
			func(r *recorder) []error { return r.read }},
		{"write with SetDeadline", net.Conn.SetDeadline,
			func(c net.Conn) error { _, err := c.Write(nil); return err },
			func(r *recorder) []error { return r.write }},
	}
	cases := []struct {
		name      string
		err       error
		deadlines []time.Time
		reported  bool
	}{
		{"failure", reset, nil, true},
		{"end of stream", io.EOF, nil, false},
		{"closed on this side", closed, nil, false},
		{"expired deadline", timeout, []time.Time{future}, true},
		{"canceled by a past deadline", timeout, []time.Time{past}, false},
		{"canceled, then cleared", timeout, []time.Time{past, {}}, true},
		{"failure while canceled", reset, []time.Time{past}, true},
	}
	for _, d := range directions {
		for _, tc := range cases {
			t.Run(d.name+"/"+tc.name, func(t *testing.T) {
				accepted := &stubConn{err: tc.err}
				r := &recorder{}
				c := accept(t, &transport.Listener{Listener: &stubListener{conns: []net.Conn{accepted}}, Observer: r})
				for _, dl := range tc.deadlines {
					if err := d.setDeadline(c, dl); err != nil {
						t.Fatal(err)
					}
				}

				if err := d.do(c); err != tc.err {
					t.Fatalf("got %v, want %v unchanged", err, tc.err)
				}
				got := d.reported(r)
				if !tc.reported {
					if len(r.all()) != 0 {
						t.Fatalf("reported %v", r.all())
					}
					return
				}
				if len(got) != 1 || got[0] != tc.err || len(r.all()) != 1 {
					t.Fatalf("reported %v, want only %v", r.all(), tc.err)
				}
				if r.conns[0] != accepted {
					t.Fatalf("hook got %T, want the accepted connection", r.conns[0])
				}
			})
		}
	}
}

// TestCanceledDirectionsAreIndependent checks that canceling reads does not hide write timeouts.
func TestCanceledDirectionsAreIndependent(t *testing.T) {
	timeout := &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}
	r := &recorder{}
	c := accept(t, &transport.Listener{Listener: &stubListener{conns: []net.Conn{&stubConn{err: timeout}}}, Observer: r})
	_ = c.SetReadDeadline(time.Unix(1, 0))
	_, _ = c.Write(nil)
	if len(r.write) != 1 {
		t.Fatalf("write timeout not reported: %v", r.all())
	}
}

// TestConnReportsFirstFailure checks that I/O failing again the same way is not reported again until the
// deadline changes, as when net/http retries a read that timed out.
func TestConnReportsFirstFailure(t *testing.T) {
	reset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	timeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"failure", reset},
		{"timeout", timeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{}
			c := accept(t, &transport.Listener{Listener: &stubListener{conns: []net.Conn{&stubConn{err: tc.err}}}, Observer: r})
			_ = c.SetReadDeadline(time.Now().Add(time.Hour))
			for range 3 {
				_, _ = c.Read(nil)
			}
			if len(r.read) != 1 {
				t.Fatalf("repeated failures reported %d times, want 1", len(r.read))
			}

			_ = c.SetReadDeadline(time.Now().Add(time.Hour))
			_, _ = c.Read(nil)
			if len(r.read) != 2 {
				t.Fatalf("failure after a new deadline reported %d times in all, want 2", len(r.read))
			}
		})
	}
}

func listenTCP(t *testing.T, observer transport.IConnObserver) *transport.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return &transport.Listener{Listener: ln, Observer: observer}
}

// TestCloseWrite checks that a wrapped connection still half-closes, which net/http relies on to deliver a
// response before closing a connection whose request it did not read (golang.org/issue/3595).
func TestCloseWrite(t *testing.T) {
	l := listenTCP(t, &recorder{})
	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	c := accept(t, l)
	defer c.Close()

	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		t.Fatalf("%T has no CloseWrite", c)
	}
	if err := cw.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := client.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("client read after CloseWrite = %d, %v; want EOF", n, err)
	}
	// The reading side stays open.
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(c, make([]byte, 1)); err != nil {
		t.Fatalf("read after CloseWrite: %v", err)
	}

	stub := accept(t, &transport.Listener{Listener: &stubListener{conns: []net.Conn{&stubConn{}}}, Observer: &recorder{}})
	if err := stub.(interface{ CloseWrite() error }).CloseWrite(); !errors.Is(err, errors.ErrUnsupported) {
		t.Fatalf("CloseWrite without support = %v, want errors.ErrUnsupported", err)
	}
}

// serveHTTP serves srv on l until the test ends.
func serveHTTP(t *testing.T, l *transport.Listener, srv *http.Server) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
}

// TestHTTPServerNormalTrafficReportsNothing runs keep-alive requests, a client close and a shutdown through
// net/http. None of them is a failure. Before the hooks ignored canceled reads, every request without a
// body reported an i/o timeout, the client close an EOF and the shutdown a closed listener.
func TestHTTPServerNormalTrafficReportsNothing(t *testing.T) {
	r := &recorder{}
	l := listenTCP(t, r)
	var (
		mu     sync.Mutex
		states []net.Conn
	)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }),
		ConnState: func(c net.Conn, _ http.ConnState) {
			mu.Lock()
			defer mu.Unlock()
			states = append(states, c)
		},
	}
	serveHTTP(t, l, srv)

	client := &http.Client{Transport: &http.Transport{}}
	url := "http://" + l.Addr().String()
	for i := range 6 {
		var resp *http.Response
		var err error
		if i%2 == 0 {
			resp, err = client.Get(url)
		} else {
			resp, err = client.Post(url, "text/plain", strings.NewReader("hello"))
		}
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	client.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if errs := r.all(); len(errs) != 0 {
		t.Fatalf("normal traffic reported %v", errs)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(states) == 0 {
		t.Fatal("no connection state reported")
	}
	if nc, ok := states[0].(interface{ NetConn() net.Conn }); !ok {
		t.Fatalf("ConnState got %T without NetConn", states[0])
	} else if _, ok := nc.NetConn().(*net.TCPConn); !ok {
		t.Fatalf("NetConn returned %T, want *net.TCPConn", nc.NetConn())
	}
}

// TestHTTPServerReportsReadTimeout checks that an expired read deadline is reported: a client that does not
// finish its request headers within ReadHeaderTimeout.
func TestHTTPServerReportsReadTimeout(t *testing.T) {
	r := &recorder{}
	l := listenTCP(t, r)
	serveHTTP(t, l, &http.Server{
		Handler:           http.NotFoundHandler(),
		ReadHeaderTimeout: 50 * time.Millisecond,
	})

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("GET / HTTP/1.1\r\n")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		read := slices.Clone(r.read)
		r.mu.Unlock()
		if len(read) > 0 {
			if len(read) != 1 || !errors.Is(read[0], os.ErrDeadlineExceeded) {
				t.Fatalf("reported %v, want one deadline error", read)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("read timeout not reported")
}

// stateRecorder is an IConnStateObserver that keeps the states it is told about.
type stateRecorder struct {
	recorder
	states []transport.ConnState
	conns  []net.Conn
}

func (r *stateRecorder) OnConnState(c net.Conn, state transport.ConnState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, state)
	r.conns = append(r.conns, c)
}

// stateOnly is told about accept errors and states, not about reads and writes.
type stateOnly struct {
	acceptObserver
	states *stateRecorder
}

func (o stateOnly) OnConnState(c net.Conn, state transport.ConnState) { o.states.OnConnState(c, state) }

// closableConn is a stubConn that can be closed.
type closableConn struct{ stubConn }

func (*closableConn) Close() error { return nil }

// TestConnState checks that a Listener with ReportState reports a connection new when it accepts it and
// closed on its first Close only, and that it wraps nothing without ReportState.
func TestConnState(t *testing.T) {
	for _, tc := range []struct {
		name        string
		observer    func(*stateRecorder) transport.IConnObserver
		reportState bool
		wantStates  []transport.ConnState
		wrapped     bool
	}{
		{"state observer", func(r *stateRecorder) transport.IConnObserver {
			return stateOnly{acceptObserver: func(error) {}, states: r}
		}, true, []transport.ConnState{transport.ConnNew, transport.ConnClosed}, true},
		{"io and state observer", func(r *stateRecorder) transport.IConnObserver { return r }, true,
			[]transport.ConnState{transport.ConnNew, transport.ConnClosed}, true},
		{"states off", func(r *stateRecorder) transport.IConnObserver {
			return stateOnly{acceptObserver: func(error) {}, states: r}
		}, false, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &stateRecorder{}
			accepted := &closableConn{stubConn{err: syscall.ECONNRESET}}
			l := &transport.Listener{
				Listener:    &stubListener{conns: []net.Conn{accepted}},
				Observer:    tc.observer(r),
				ReportState: tc.reportState,
			}
			c := accept(t, l)
			if wrapped := c != net.Conn(accepted); wrapped != tc.wrapped {
				t.Fatalf("wrapped %v, want %v", wrapped, tc.wrapped)
			}
			_ = c.Close()
			_ = c.Close()

			if !slices.Equal(r.states, tc.wantStates) {
				t.Fatalf("states %v, want %v", r.states, tc.wantStates)
			}
			for _, reported := range r.conns {
				if reported != accepted {
					t.Fatalf("reported %T, want the accepted connection", reported)
				}
			}
		})
	}
}

func TestConnStateString(t *testing.T) {
	for state, want := range map[transport.ConnState]string{
		transport.ConnNew:      "new",
		transport.ConnActive:   "active",
		transport.ConnIdle:     "idle",
		transport.ConnHijacked: "hijacked",
		transport.ConnClosed:   "closed",
		transport.ConnState(9): "unknown",
	} {
		if got := state.String(); got != want {
			t.Errorf("ConnState(%d) = %q, want %q", state, got, want)
		}
	}
}
