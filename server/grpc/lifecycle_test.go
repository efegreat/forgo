package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
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

// startServer runs Serve in the background, waits until the server answers, and returns a client and what
// Serve returns.
func startServer(t *testing.T, s *server, opts ...grpc.DialOption) (testpb.TestServiceClient, <-chan error) {
	t.Helper()
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	if opts == nil {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(s.cfg.Addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := testpb.NewTestServiceClient(conn)
	// An unknown method answers as soon as the server serves.
	for range 100 {
		err := conn.Invoke(testCtx(t), "/forgo.Probe/Probe", &testpb.Empty{}, &testpb.Empty{})
		if status.Code(err) == codes.Unimplemented {
			return client, served
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return nil, nil
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

// blockingConfig serves UnaryCall with a handler that reports on started, then waits for release, or for its
// context to end when honorCtx is set.
func blockingConfig(addr string, started chan<- struct{}, release <-chan struct{}, honorCtx bool) Config {
	return Config{Addr: addr, Services: []ServiceConfig{unaryConfig(func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		started <- struct{}{}
		if !honorCtx {
			<-release
			return &testpb.SimpleResponse{}, nil
		}
		select {
		case <-release:
			return &testpb.SimpleResponse{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})}}
}

func TestServeAfterTerminate(t *testing.T) {
	s := newServer(t, Config{Addr: freeAddr(t), Services: []ServiceConfig{testServiceConfig()}}, io.Discard)
	if err := s.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	waitServed(t, served)
}

func TestServeError(t *testing.T) {
	s := newServer(t, Config{Network: "bogus", Services: []ServiceConfig{testServiceConfig()}}, io.Discard)
	if err := s.Serve(); err == nil {
		t.Fatal("served on an unknown network")
	}
}

func TestTerminateWaitsForCalls(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	s := newServer(t, blockingConfig(freeAddr(t), started, release, true), io.Discard)
	client, served := startServer(t, s)

	called := make(chan error, 1)
	go func() {
		_, err := client.UnaryCall(context.Background(), &testpb.SimpleRequest{})
		called <- err
	}()
	<-started

	terminated := make(chan error, 1)
	go func() { terminated <- s.Terminate(context.Background()) }()
	select {
	case err := <-terminated:
		t.Fatalf("Terminate returned during a call: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-terminated; err != nil {
		t.Fatal(err)
	}
	if err := <-called; err != nil {
		t.Fatalf("the call in flight failed: %v", err)
	}
	waitServed(t, served)
}

// grpc-go's GracefulStop waits for a handler whose client canceled, although its stream is gone. Resources
// would otherwise close under the handler.
func TestTerminateWaitsForCanceledCall(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	s := newServer(t, blockingConfig(freeAddr(t), started, release, false), io.Discard)
	client, served := startServer(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan error, 1)
	go func() {
		_, err := client.UnaryCall(ctx, &testpb.SimpleRequest{})
		called <- err
	}()
	<-started
	cancel()
	if err := <-called; status.Code(err) != codes.Canceled {
		t.Fatalf("got %v", err)
	}

	terminated := make(chan error, 1)
	go func() { terminated <- s.Terminate(context.Background()) }()
	select {
	case err := <-terminated:
		t.Fatalf("Terminate returned while a handler runs: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-terminated; err != nil {
		t.Fatal(err)
	}
	waitServed(t, served)
}

func TestTerminateTimeoutThenKill(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	s := newServer(t, blockingConfig(freeAddr(t), started, release, true), io.Discard)
	client, served := startServer(t, s)

	called := make(chan error, 1)
	go func() {
		_, err := client.UnaryCall(context.Background(), &testpb.SimpleRequest{})
		called <- err
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Terminate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	s.Kill()
	waitServed(t, served)
	if err := <-called; err == nil {
		t.Fatal("the killed call succeeded")
	}
}

// A long stream ends when the server terminates, through State.Terminating.
func TestTerminatingEndsStreams(t *testing.T) {
	started := make(chan struct{}, 1)
	svc := ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{
			Method: "StreamingOutputCall",
			Handler: ServerStreamAs[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse](
				func() ServerStreamHandler[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse] {
					return func(ctx context.Context, _ *testpb.StreamingOutputCallRequest, _ func(*testpb.StreamingOutputCallResponse) error) error {
						op, _ := operation.FromCtx(ctx)
						started <- struct{}{}
						<-op.State.(*State).Terminating()
						return nil
					}
				}),
		}},
	}
	s := newServer(t, Config{Addr: freeAddr(t), Services: []ServiceConfig{svc}}, io.Discard)
	client, served := startServer(t, s)
	stream, err := client.StreamingOutputCall(context.Background(), &testpb.StreamingOutputCallRequest{})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v", err)
	}
	waitServed(t, served)
}

func TestInitStartsNoGoroutines(t *testing.T) {
	cfg := Config{Reflection: true, Services: []ServiceConfig{testServiceConfig(), HealthService()}}
	before := runtime.NumGoroutine()
	newServer(t, cfg, io.Discard)
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("%d goroutines before New and Init, %d after", before, after)
	}
}

// ioObserver records the errors a transport.IConnIOObserver is told about.
type ioObserver struct {
	mu     sync.Mutex
	errors []error
}

func (o *ioObserver) OnAcceptError(err error) { o.record(err) }
func (o *ioObserver) OnReadError(_ net.Conn, err error) {
	o.record(err)
}
func (o *ioObserver) OnWriteError(_ net.Conn, err error) {
	o.record(err)
}

func (o *ioObserver) record(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errors = append(o.errors, err)
}

// A graceful stop is not a connection failure.
func TestObserverCleanStop(t *testing.T) {
	obs := &ioObserver{}
	s := newServer(t, Config{Addr: freeAddr(t), Observer: obs, Services: []ServiceConfig{testServiceConfig()}}, io.Discard)
	client, served := startServer(t, s)
	if _, err := client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Terminate(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	waitServed(t, served)
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.errors) != 0 {
		t.Fatalf("reported %v", obs.errors)
	}
}

func TestDrainObserver(t *testing.T) {
	inner := &ioObserver{}
	terminating := make(chan struct{})
	o := drainObserver{IConnIOObserver: inner, terminating: terminating}
	reset := &net.OpError{Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
	pipe := &net.OpError{Op: "write", Err: os.NewSyscallError("write", syscall.EPIPE)}
	timeout := os.ErrDeadlineExceeded

	o.OnReadError(nil, reset)
	o.OnWriteError(nil, pipe)
	close(terminating)
	o.OnReadError(nil, reset)   // dropped
	o.OnWriteError(nil, pipe)   // dropped
	o.OnReadError(nil, timeout) // still a failure
	o.OnAcceptError(timeout)

	inner.mu.Lock()
	defer inner.mu.Unlock()
	want := []error{reset, pipe, timeout, timeout}
	if len(inner.errors) != len(want) {
		t.Fatalf("reported %v, want %v", inner.errors, want)
	}
	for i := range want {
		if inner.errors[i] != want[i] {
			t.Fatalf("reported %v, want %v", inner.errors, want)
		}
	}

	// Only an IConnIOObserver is wrapped.
	s := newServer(t, Config{Observer: acceptObserver{}, Services: []ServiceConfig{testServiceConfig()}}, io.Discard)
	if _, ok := s.observer().(acceptObserver); !ok {
		t.Fatalf("wrapped %T", s.observer())
	}
}

type acceptObserver struct{}

func (acceptObserver) OnAcceptError(error) {}

func TestServeTLS(t *testing.T) {
	s := newServer(t, Config{
		Addr:      freeAddr(t),
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{selfSignedCert(t)}},
		Services:  []ServiceConfig{testServiceConfig()},
	}, io.Discard)
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	client, served := startServer(t, s, grpc.WithTransportCredentials(creds))
	if _, err := client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Terminate(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	waitServed(t, served)
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
