package grpc

import (
	"context"
	"errors"
	"io"
	"iter"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ctxKey struct{}

// A grpc-go unary interceptor runs inside the chain: it sees and replaces the request and the response, passes
// a context on to the handler, and its rejections are logged and answered like any other error.
func TestUnaryInterceptor(t *testing.T) {
	var streamCalls atomic.Int32
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/grpc.testing.TestService/StreamingOutputCall" {
			streamCalls.Add(1)
		}
		if len(metadata.ValueFromIncomingContext(ctx, "x-tenant")) == 0 {
			return nil, status.Error(codes.Unauthenticated, "no tenant")
		}
		in := req.(*testpb.SimpleRequest)
		in.ResponseSize = 7
		resp, err := handler(context.WithValue(ctx, ctxKey{}, "from interceptor"), &testpb.SimpleRequest{
			ResponseSize: in.GetResponseSize(),
			Payload:      in.GetPayload(),
		})
		if err != nil {
			return nil, err
		}
		out := resp.(*testpb.SimpleResponse)
		out.Hostname = "intercepted"
		return out, nil
	}
	handler := func(ctx context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		op, _ := operation.FromCtx(ctx)
		if op == nil || ctx.Value(ctxKey{}) != "from interceptor" || req.GetResponseSize() != 7 {
			return nil, errors.New("the interceptor's context or request did not reach the handler")
		}
		return &testpb.SimpleResponse{Username: "ada"}, nil
	}
	svc := unaryConfig(handler)
	svc.Methods = append(svc.Methods, MethodConfig{
		Method: "StreamingOutputCall", Handler: ServerStreamFor((*testService).StreamingOutputCall),
	})
	cfg := Config{Services: []ServiceConfig{svc}}
	cfg.Middlewares = []forgo.MiddlewareFactory{UnaryInterceptor(interceptor)}
	h := newHarness(t, cfg)

	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	assertStatus(t, err, codes.Unauthenticated, "no tenant", "")
	if failed := h.line(t, "operation failed"); failed["err_code"] != "unauthenticated" || failed["grpc_code"] != "Unauthenticated" {
		t.Fatalf("logged %v", failed)
	}

	ctx := metadata.AppendToOutgoingContext(testCtx(t), "x-tenant", "acme")
	resp, err := h.client.UnaryCall(ctx, &testpb.SimpleRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetUsername() != "ada" || resp.GetHostname() != "intercepted" {
		t.Fatalf("got %v", resp)
	}
	// The Recorder logs the response the client got.
	if body := h.line(t, "operation completed")["resp_body"]; !jsonEqual(body, `{"username":"ada","hostname":"intercepted"}`) {
		t.Fatalf("resp_body %v", body)
	}

	// Streams are not the unary interceptor's.
	stream, err := h.client.StreamingOutputCall(testCtx(t), &testpb.StreamingOutputCallRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v", err)
	}
	if streamCalls.Load() != 0 {
		t.Fatal("a unary interceptor ran on a stream")
	}
}

// A unary interceptor may answer without calling its handler, such as from a cache.
func TestUnaryInterceptorAnswers(t *testing.T) {
	var called atomic.Bool
	handler := func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		called.Store(true)
		return &testpb.SimpleResponse{}, nil
	}
	for _, tc := range []struct {
		name string
		resp any
		code codes.Code
	}{
		{"cached", &testpb.SimpleResponse{Username: "cached"}, codes.OK},
		{"nil", nil, codes.Internal},
		{"not a message", "cached", codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := UnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
				return tc.resp, nil
			})
			h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(handler, cache)}})
			resp, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
			if status.Code(err) != tc.code || (err == nil && resp.GetUsername() != "cached") {
				t.Fatalf("got %v, %v", resp, err)
			}
			if tc.resp == nil {
				assertStatus(t, err, codes.Internal, "handler returned no response", "nil_response")
			}
		})
	}
	if called.Load() {
		t.Fatal("the handler ran")
	}

	// A request the handler cannot take fails the call.
	bad := UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, "not a message")
	})
	h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(handler, bad)}})
	if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); status.Code(err) != codes.Internal {
		t.Fatalf("got %v", err)
	}
}

// countingStream counts the messages received and sent on a stream, as a stream interceptor's wrapper does.
type countingStream struct {
	grpc.ServerStream
	ctx            context.Context
	received, sent *atomic.Int32
}

func (s *countingStream) Context() context.Context { return s.ctx }

func (s *countingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.received.Add(1)
	}
	return err
}

func (s *countingStream) SendMsg(m any) error {
	s.sent.Add(1)
	return s.ServerStream.SendMsg(m)
}

// A grpc-go stream interceptor wraps the stream the handler receives and sends on, and passes a context on.
func TestStreamInterceptor(t *testing.T) {
	var received, sent atomic.Int32
	var mu sync.Mutex
	var infos []grpc.StreamServerInfo
	interceptor := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		mu.Lock()
		infos = append(infos, *info)
		mu.Unlock()
		ctx := context.WithValue(ss.Context(), ctxKey{}, "from interceptor")
		return handler(srv, &countingStream{ServerStream: ss, ctx: ctx, received: &received, sent: &sent})
	}
	var seen atomic.Value
	svc := testServiceConfig()
	svc.Methods[4].Handler = BidiStreamAs[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse](
		func(s *testService) BidiStreamHandler[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse] {
			return func(ctx context.Context, reqs iter.Seq2[*testpb.StreamingOutputCallRequest, error], send func(*testpb.StreamingOutputCallResponse) error) error {
				seen.Store(ctx.Value(ctxKey{}))
				return s.FullDuplexCall(ctx, reqs, send)
			}
		})
	cfg := Config{Services: []ServiceConfig{svc}}
	cfg.Middlewares = []forgo.MiddlewareFactory{StreamInterceptor(interceptor)}
	h := newHarness(t, cfg)

	if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}
	stream, err := h.client.FullDuplexCall(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := stream.Send(&testpb.StreamingOutputCallRequest{}); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	_ = stream.CloseSend()
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v", err)
	}
	if received.Load() != 2 || sent.Load() != 2 || seen.Load() != "from interceptor" {
		t.Fatalf("received %d, sent %d, context value %v", received.Load(), sent.Load(), seen.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	want := []grpc.StreamServerInfo{{FullMethod: "/grpc.testing.TestService/FullDuplexCall", IsClientStream: true, IsServerStream: true}}
	if !slices.Equal(infos, want) {
		t.Fatalf("infos %+v", infos)
	}
}

func TestInterceptorFactories(t *testing.T) {
	type prefix string
	boot := forgo.NewBootstrap(nil, []any{func() prefix { return "x" }})
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = boot.Close() }()

	var got []prefix
	unary := UnaryInterceptorAs(func(p prefix) grpc.UnaryServerInterceptor {
		got = append(got, p)
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	})
	stream := StreamInterceptorAs(func(p prefix) grpc.StreamServerInterceptor {
		got = append(got, p+"s")
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, ss)
		}
	})
	for _, f := range []forgo.MiddlewareFactory{unary, stream} {
		if m, err := f(r); err != nil || m == nil {
			t.Fatalf("got %v, %v", m, err)
		}
	}
	if !slices.Equal(got, []prefix{"x", "xs"}) {
		t.Fatalf("got %v", got)
	}

	for _, f := range []forgo.MiddlewareFactory{
		UnaryInterceptor(nil),
		StreamInterceptor(nil),
		UnaryInterceptorAs(func() grpc.UnaryServerInterceptor { return nil }),
		StreamInterceptorAs(func(int) grpc.StreamServerInterceptor { return nil }),
	} {
		if _, err := f(r); err == nil {
			t.Fatal("built a nil or unresolvable interceptor")
		}
	}
}

// A middleware replaces the response after its next returns, or answers without calling next.
func TestMiddlewareResponse(t *testing.T) {
	var called atomic.Bool
	handler := func(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		called.Store(true)
		if req.GetFillUsername() {
			return &testpb.SimpleResponse{Username: "filled"}, nil
		}
		return &testpb.SimpleResponse{Username: "handler"}, nil
	}
	middleware := func(fn func(st *State, next operation.Handler, op *operation.Context) error) forgo.MiddlewareFactory {
		return forgo.MiddlewareAs(func() operation.Middleware {
			return func(next operation.Handler) operation.Handler {
				return func(op *operation.Context) error {
					return fn(op.State.(*State), next, op)
				}
			}
		})
	}
	for _, tc := range []struct {
		name     string
		mw       forgo.MiddlewareFactory
		username string
		code     codes.Code
		called   bool
	}{
		{"replaced", middleware(func(st *State, next operation.Handler, op *operation.Context) error {
			if err := next(op); err != nil {
				return err
			}
			st.Response = &testpb.SimpleResponse{Username: "replaced"}
			return nil
		}), "replaced", codes.OK, true},
		{"answered", middleware(func(st *State, _ operation.Handler, _ *operation.Context) error {
			st.Response = &testpb.SimpleResponse{Username: "cached"}
			return nil
		}), "cached", codes.OK, false},
		{"no response", middleware(func(*State, operation.Handler, *operation.Context) error {
			return nil
		}), "", codes.Internal, false},
		{"request replaced", middleware(func(st *State, next operation.Handler, op *operation.Context) error {
			if st.ClientStreams() || st.ServerStreams() {
				return errors.New("a unary method streams")
			}
			st.Request = &testpb.SimpleRequest{FillUsername: true}
			return next(op)
		}), "filled", codes.OK, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called.Store(false)
			h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(handler, tc.mw)}})
			resp, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
			if status.Code(err) != tc.code || resp.GetUsername() != tc.username || called.Load() != tc.called {
				t.Fatalf("got %v, %v, handler called %v", resp, err, called.Load())
			}
			// The Responder answers, not grpc-go's check of the unary cardinality.
			if tc.code != codes.OK {
				assertStatus(t, err, codes.Internal, "handler returned no response", "nil_response")
			}
		})
	}
}

// A middleware sets the header and trailer of the response, and wraps the stream.
func TestStateHeadersAndStream(t *testing.T) {
	var received atomic.Int32
	var sent atomic.Int32
	mw := forgo.MiddlewareAs(func() operation.Middleware {
		return func(next operation.Handler) operation.Handler {
			return func(op *operation.Context) error {
				st := op.State.(*State)
				if err := st.SetHeader(metadata.Pairs("x-region", "eu")); err != nil {
					return err
				}
				st.SetTrailer(metadata.Pairs("x-cost", "3"))
				if st.ClientStreams() {
					st.WrapStream(func(ss grpc.ServerStream) grpc.ServerStream {
						return &countingStream{ServerStream: ss, ctx: ss.Context(), received: &received, sent: &sent}
					})
				}
				if err := next(op); err != nil {
					return err
				}
				// The Responder has not sent the response yet.
				return st.SendHeader(metadata.Pairs("x-late", "1"))
			}
		}
	})
	svc := testServiceConfig()
	svc.Middlewares = []forgo.MiddlewareFactory{mw}
	h := newHarness(t, Config{Services: []ServiceConfig{svc}})

	var header, trailer metadata.MD
	if _, err := h.client.EmptyCall(testCtx(t), &testpb.Empty{}, grpc.Header(&header), grpc.Trailer(&trailer)); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(header.Get("x-region"), []string{"eu"}) || !slices.Equal(header.Get("x-late"), []string{"1"}) ||
		!slices.Equal(trailer.Get("x-cost"), []string{"3"}) {
		t.Fatalf("header %v, trailer %v", header, trailer)
	}

	// Once a stream sent a message, the header is sent.
	stream, err := h.client.StreamingOutputCall(testCtx(t), &testpb.StreamingOutputCallRequest{
		ResponseParameters: []*testpb.ResponseParameters{{Size: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = stream.Recv()
	if _, err := stream.Recv(); status.Code(err) != codes.Internal {
		t.Fatalf("got %v", err)
	}

	input, err := h.client.StreamingInputCall(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := input.Send(&testpb.StreamingInputCallRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := input.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	if received.Load() != 3 {
		t.Fatalf("the wrapped stream received %d messages", received.Load())
	}
}
