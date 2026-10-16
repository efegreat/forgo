package http

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
)

// freeAddr returns a local TCP address nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startServer runs Serve in the background, waits until the server answers, and returns what Serve returns.
func startServer(t *testing.T, s *server) <-chan error {
	t.Helper()
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	for range 100 {
		resp, err := http.Get("http://" + s.cfg.Addr + "/health")
		if err == nil {
			_ = resp.Body.Close()
			return served
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return nil
}

// waitServed waits for Serve to return and fails the test unless it returned nil.
func waitServed(t *testing.T, served <-chan error) {
	t.Helper()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}
}

// blockingRoutes returns testRoutes and a /block route whose handler reports on started, then waits for release
// or for its context to end, which it reports on canceled.
func blockingRoutes(started, canceled chan<- struct{}, release <-chan struct{}) []RouteConfig {
	block := HandlerAs[struct{}, struct{}](func() Handler[struct{}, struct{}] {
		return func(ctx context.Context, _ struct{}) (struct{}, error) {
			started <- struct{}{}
			select {
			case <-release:
				return struct{}{}, nil
			case <-ctx.Done():
				canceled <- struct{}{}
				return struct{}{}, ctx.Err()
			}
		}
	})
	return append(testRoutes(), RouteConfig{Name: "block", Method: "GET", Path: "/block", Handler: block})
}

func TestServeAfterTerminate(t *testing.T) {
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: testRoutes()})
	if err := s.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	waitServed(t, served)
}

func TestServeListenError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	s := newTestServer(t, Config{Addr: ln.Addr().String(), Routes: testRoutes()})
	if err := s.Serve(); err == nil {
		t.Fatal("Serve on a taken address returned nil")
	}
}

// TestTerminateWaitsForRequests checks that Terminate returns only once the request in flight is answered.
func TestTerminateWaitsForRequests(t *testing.T) {
	started, canceled, release := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: blockingRoutes(started, canceled, release)})
	served := startServer(t, s)

	answered := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + s.cfg.Addr + "/block")
		if err != nil {
			answered <- 0
			return
		}
		_ = resp.Body.Close()
		answered <- resp.StatusCode
	}()
	<-started

	terminated := make(chan error, 1)
	go func() { terminated <- s.Terminate(context.Background()) }()
	select {
	case err := <-terminated:
		t.Fatalf("Terminate returned %v while a request was in flight", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-terminated; err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if status := <-answered; status != http.StatusOK {
		t.Fatalf("request answered %d, want 200", status)
	}
	waitServed(t, served)
}

// TestKillClosesConnections checks that Terminate gives up when its context ends, and that Kill then closes the
// connection of the request in flight, which cancels its context.
func TestKillClosesConnections(t *testing.T) {
	started, canceled, release := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: blockingRoutes(started, canceled, release)})
	served := startServer(t, s)

	failed := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + s.cfg.Addr + "/block")
		if err == nil {
			_ = resp.Body.Close()
		}
		failed <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Terminate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("terminate: %v, want the context's error", err)
	}

	s.Kill()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("Kill did not cancel the request's context")
	}
	if err := <-failed; err == nil {
		t.Fatal("the client got a response from a killed server")
	}
	waitServed(t, served)
}

// TestTerminatingClosesOnTerminate checks that a long-lived response, such as server-sent events, learns
// from State.Terminating that the server terminates, so that Terminate does not wait for its deadline.
func TestTerminatingClosesOnTerminate(t *testing.T) {
	started := make(chan struct{}, 1)
	events := HandlerAs[struct{}, struct{}](func() Handler[struct{}, struct{}] {
		return func(ctx context.Context, _ struct{}) (struct{}, error) {
			op, _ := operation.FromCtx(ctx)
			started <- struct{}{}
			<-op.State.(*State).Terminating()
			return struct{}{}, nil
		}
	})
	routes := append(testRoutes(), RouteConfig{Name: "events", Method: "GET", Path: "/events", Handler: events})
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: routes})
	served := startServer(t, s)

	answered := make(chan int, 1)
	go func() {
		resp, err := http.Get("http://" + s.cfg.Addr + "/events")
		if err != nil {
			answered <- 0
			return
		}
		_ = resp.Body.Close()
		answered <- resp.StatusCode
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Terminate(ctx); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	if status := <-answered; status != http.StatusOK {
		t.Fatalf("request answered %d, want 200", status)
	}
	waitServed(t, served)
}

// hijackRoutes returns testRoutes and a /hijack route whose handler hijacks the connection, answers 101 on it,
// and runs then, before it closes the connection and returns nil.
func hijackRoutes(then func(st *State, conn net.Conn)) []RouteConfig {
	hijack := HandlerAs[struct{}, struct{}](func() Handler[struct{}, struct{}] {
		return func(ctx context.Context, _ struct{}) (struct{}, error) {
			op, _ := operation.FromCtx(ctx)
			st := op.State.(*State)
			conn, rw, err := http.NewResponseController(st.Writer).Hijack()
			if err != nil {
				return struct{}{}, err
			}
			defer conn.Close()
			_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
			_ = rw.Flush()
			then(st, conn)
			return struct{}{}, nil
		}
	})
	return append(testRoutes(), RouteConfig{Name: "hijack", Method: "GET", Path: "/hijack", Handler: hijack})
}

// dialHijack sends a request to /hijack and reads the 101 its handler answers.
func dialHijack(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := io.WriteString(conn, "GET /hijack HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("answered %d, want 101", resp.StatusCode)
	}
	return conn
}

// lockedBuffer is a bytes.Buffer that is safe for concurrent use, for net/http's error log.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestTerminateWaitsForHijacked checks that Terminate closes State.Terminating and waits for the handler of a
// hijacked connection to return, which http.Server.Shutdown does not, and that the server writes no response
// on a hijacked connection.
func TestTerminateWaitsForHijacked(t *testing.T) {
	terminating, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	routes := hijackRoutes(func(st *State, _ net.Conn) {
		<-st.Terminating()
		close(terminating)
		<-release
		close(returned)
	})
	errorLog := &lockedBuffer{}
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: routes, ErrorLog: log.New(errorLog, "", 0)})
	served := startServer(t, s)
	dialHijack(t, s.cfg.Addr)

	terminated := make(chan error, 1)
	go func() { terminated <- s.Terminate(context.Background()) }()
	<-terminating
	select {
	case err := <-terminated:
		t.Fatalf("Terminate returned %v while the handler of a hijacked connection ran", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-terminated; err != nil {
		t.Fatalf("terminate: %v", err)
	}
	select {
	case <-returned:
	default:
		t.Fatal("Terminate returned before the handler")
	}
	waitServed(t, served)
	if logged := errorLog.String(); logged != "" {
		t.Fatalf("net/http logged %q", logged)
	}
}

// TestKillClosesHijacked checks that Terminate gives up on a hijacked connection when its context ends, and
// that Kill then closes the connection, so that its handler's reads fail.
func TestKillClosesHijacked(t *testing.T) {
	started, readFailed := make(chan struct{}), make(chan error, 1)
	routes := hijackRoutes(func(_ *State, conn net.Conn) {
		close(started)
		_, err := conn.Read(make([]byte, 1))
		readFailed <- err
	})
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: routes})
	served := startServer(t, s)
	dialHijack(t, s.cfg.Addr)
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Terminate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("terminate: %v, want the context's error", err)
	}

	s.Kill()
	select {
	case err := <-readFailed:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("read: %v, want net.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Kill did not close the hijacked connection")
	}
	waitServed(t, served)
}

// Outside the server, Hijack reaches the writer below the recorder, and a connection is hijacked once.
func TestHijackOutsideServer(t *testing.T) {
	var _ http.Hijacker = (*ResponseRecorder)(nil)
	if _, _, err := newResponseRecorder(httptest.NewRecorder(), 0).Hijack(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("hijack: %v, want http.ErrNotSupported", err)
	}

	server, client := net.Pipe()
	defer client.Close()
	rec := newResponseRecorder(hijackableWriter{ResponseWriter: httptest.NewRecorder(), conn: server}, 0)
	if conn, _, err := rec.Hijack(); err != nil || conn != server {
		t.Fatalf("hijack: %v, %v", conn, err)
	}
	if !rec.Written() {
		t.Fatal("a hijacked response is not written")
	}
	if _, _, err := rec.Hijack(); !errors.Is(err, http.ErrHijacked) {
		t.Fatalf("second hijack: %v, want http.ErrHijacked", err)
	}
}

// hijackableWriter is an http.ResponseWriter that hands conn over.
type hijackableWriter struct {
	http.ResponseWriter
	conn net.Conn
}

func (w hijackableWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.conn, bufio.NewReadWriter(bufio.NewReader(w.conn), bufio.NewWriter(w.conn)), nil
}

// A connection that net/http hijacks while the server is killed is closed as soon as it is recorded.
func TestHijackedAfterKill(t *testing.T) {
	var h hijacks
	w := &ResponseRecorder{}
	h.start(w)
	h.kill()

	server, client := net.Pipe()
	defer client.Close()
	h.hijacked(w, server)
	if _, err := server.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write: %v, want the connection closed", err)
	}

	h.done(w)
	if err := h.wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
}
