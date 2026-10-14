package http

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
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
