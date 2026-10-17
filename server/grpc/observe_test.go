package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
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
