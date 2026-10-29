package grpc

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// newGRPCObserver returns an observer that records the code of a call's State when End is called.
func newGRPCObserver() *observertest.Observer {
	return &observertest.Observer{State: func(st any) any { return st.(*State).Code() }}
}

// TestOperationObserver checks that the observer sees every call once, with the code the client got already
// chosen, and that the status of the observed error is the one of that code.
func TestOperationObserver(t *testing.T) {
	unary := func(h *harness) error {
		_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
		return err
	}
	failing := func(err error) Config {
		return Config{Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return nil, err
		})}}
	}
	unaryCall := "/grpc.testing.TestService/UnaryCall"

	for _, tc := range []struct {
		name     string
		cfg      Config
		call     func(h *harness) error
		wantName string
		wantCode codes.Code
	}{
		{"ok", Config{Services: []ServiceConfig{testServiceConfig()}}, unary, unaryCall, codes.OK},
		{"operation error", failing(operation.Error{Code: "denied", Status: operation.StatusPermissionDenied, Message: "denied"}),
			unary, unaryCall, codes.PermissionDenied},
		{"status error", failing(status.Error(codes.NotFound, "gone")), unary, unaryCall, codes.NotFound},
		{"plain error", failing(errors.New("boom")), unary, unaryCall, codes.Internal},
		{"panic", Config{Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			panic("boom")
		})}}, unary, unaryCall, codes.Internal},
		// grpc-go answers the failed send itself.
		{"send failed", Config{MaxSendMsgSize: 64, Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return &testpb.SimpleResponse{Payload: &testpb.Payload{Body: make([]byte, 128)}}, nil
		})}}, unary, unaryCall, codes.ResourceExhausted},
		{"unimplemented", Config{Services: []ServiceConfig{testServiceConfig()}}, func(h *harness) error {
			return h.conn.Invoke(testCtx(t), "/grpc.testing.TestService/UnimplementedCall", &testpb.Empty{}, &testpb.Empty{})
		}, unimplementedName, codes.Unimplemented},
		{"server stream", Config{Services: []ServiceConfig{testServiceConfig()}}, func(h *harness) error {
			stream, err := h.client.StreamingOutputCall(testCtx(t), &testpb.StreamingOutputCallRequest{
				ResponseParameters: []*testpb.ResponseParameters{{Size: 1}, {Size: 2}, {Size: 3}},
			})
			if err != nil {
				return err
			}
			for {
				if _, err := stream.Recv(); err != nil {
					return ignoreEOF(err)
				}
			}
		}, "/grpc.testing.TestService/StreamingOutputCall", codes.OK},
		{"bidi stream", Config{Services: []ServiceConfig{testServiceConfig()}}, func(h *harness) error {
			stream, err := h.client.FullDuplexCall(testCtx(t))
			if err != nil {
				return err
			}
			for range 3 {
				if err := stream.Send(&testpb.StreamingOutputCallRequest{}); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
			}
			if err := stream.CloseSend(); err != nil {
				return err
			}
			_, err = stream.Recv()
			return ignoreEOF(err)
		}, "/grpc.testing.TestService/FullDuplexCall", codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := newGRPCObserver()
			h := newHarness(t, tc.cfg, obs.Provider())
			err := tc.call(h)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("answered %v (%v), want %v", got, err, tc.wantCode)
			}

			// One observation per call, streams included.
			e := obs.Last(t, 1)
			if e.Type != OperationTypeGRPCHandler || e.Name != tc.wantName {
				t.Fatalf("observed %s %q, want %q", e.Type, e.Name, tc.wantName)
			}
			if e.State != tc.wantCode {
				t.Fatalf("observed State.Code() %v, want %v", e.State, tc.wantCode)
			}
			// The labels taken from the error are those of the status the client got.
			if (e.Err == nil) != (tc.wantCode == codes.OK) {
				t.Fatalf("observed error %v for %v", e.Err, tc.wantCode)
			}
			if e.Err != nil && gRPCStatus(e.Error.Status) != tc.wantCode {
				t.Fatalf("observed status %s, which is %v, but the client got %v", e.Error.Status, gRPCStatus(e.Error.Status), tc.wantCode)
			}
		})
	}
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func TestOperationObserverWithoutBaseMiddlewares(t *testing.T) {
	obs := newGRPCObserver()
	cfg := Config{Services: []ServiceConfig{testServiceConfig()}}
	cfg.DisableBaseMiddlewares = true
	h := newHarness(t, cfg, obs.Provider())
	if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}
	if obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatal("observed a call without base middlewares")
	}
}

func TestOperationObserverAmbiguous(t *testing.T) {
	a, b := &observertest.Observer{}, &observertest.Observer{}
	err := newServerErr(t, Config{Services: []ServiceConfig{testServiceConfig()}}, a.Provider(), func() *observertest.Observer { return b })
	if !errors.Is(err, forgo.ErrAmbiguousDependency) {
		t.Fatalf("New = %v, want ErrAmbiguousDependency", err)
	}
}

type nopObserver struct{}

func (nopObserver) Start(*operation.Context)                      {}
func (nopObserver) End(*operation.Context, operation.Observation) {}

// serveConns serves s on an in-memory listener through s.listener, so that its connection observer is used,
// and returns a client connected to it.
func serveConns(t *testing.T, s *server) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = s.g.Serve(s.listener(lis))
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		s.g.Stop()
		<-served
	})
	return conn
}

// TestConnObserver checks that the server asks the bootstrap's observer for a connection observer once,
// without ReportsActivity, and reports a connection new when it is accepted and closed once, whether the
// client or Kill closes it.
func TestConnObserver(t *testing.T) {
	for _, kill := range []bool{false, true} {
		obs := &observertest.ConnObserver{}
		s := newServer(t, Config{Addr: "127.0.0.1:9191", Services: []ServiceConfig{testServiceConfig()}}, io.Discard, obs.Provider())
		conn := serveConns(t, s)
		if _, err := testpb.NewTestServiceClient(conn).UnaryCall(testCtx(t), &testpb.SimpleRequest{}); err != nil {
			t.Fatal(err)
		}
		if kill {
			s.Kill()
		} else {
			_ = conn.Close()
		}

		states := obs.Conns.WaitClosed(t, 1)
		if len(states) != 1 || !slices.Equal(states[0], []transport.ConnState{transport.ConnNew, transport.ConnClosed}) {
			t.Fatalf("kill=%v: states %v, want one connection, new then closed", kill, states)
		}
		if servers := obs.Servers(); !slices.Equal(servers, []transport.Server{{Name: ServerNameGRPC, Addr: "127.0.0.1:9191"}}) {
			t.Fatalf("asked for %v", servers)
		}
	}
}

// ioStateObserver is told about read and write errors and about states.
type ioStateObserver struct {
	ioObserver
	observertest.Conns
}

func (o *ioStateObserver) OnAcceptError(err error) { o.ioObserver.OnAcceptError(err) }

// TestConnObserverDrain checks that the drainObserver of an observer of read and write errors still reports
// the states of one that observes them too, and only of such an observer.
func TestConnObserverDrain(t *testing.T) {
	both := &ioStateObserver{}
	s := newServer(t, Config{Observer: both, Services: []ServiceConfig{testServiceConfig()}}, io.Discard)
	s.terminateOnce.Do(func() { close(s.terminating) })
	o, ok := s.connObserver().(transport.IConnStateObserver)
	if !ok {
		t.Fatalf("%T reports no states", s.connObserver())
	}
	c, _ := net.Pipe()
	o.OnConnState(c, transport.ConnClosed)
	if states := both.Conns.States(); len(states) != 1 || !slices.Equal(states[0], []transport.ConnState{transport.ConnClosed}) {
		t.Fatalf("states %v", states)
	}

	ioOnly := newServer(t, Config{Observer: &ioObserver{}, Services: []ServiceConfig{testServiceConfig()}}, io.Discard)
	if _, ok := ioOnly.connObserver().(transport.IConnStateObserver); ok {
		t.Fatal("the drainObserver of an observer of read and write errors reports states")
	}
}

// TestConnObserverNamed checks that a named server asks for the connection observer of its own name.
func TestConnObserverNamed(t *testing.T) {
	obs := &observertest.ConnObserver{}
	cfg := Config{ServerConfig: forgo.ServerConfig{Name: "internal"}, Addr: "127.0.0.1:9191", Services: []ServiceConfig{testServiceConfig()}}
	newServer(t, cfg, io.Discard, obs.Provider())
	if servers := obs.Servers(); !slices.Equal(servers, []transport.Server{{Name: "internal", Addr: "127.0.0.1:9191"}}) {
		t.Fatalf("asked for %v", servers)
	}
}
