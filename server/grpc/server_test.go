package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// testService implements the methods of grpc.testing.TestService that the tests serve.
type testService struct{}

func (*testService) UnaryCall(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return &testpb.SimpleResponse{Username: "ada", Payload: req.GetPayload()}, nil
}

func (*testService) EmptyCall(context.Context, *testpb.Empty) (*testpb.Empty, error) {
	return &testpb.Empty{}, nil
}

// StreamingOutputCall sends one response per response parameter, with a body of its size.
func (*testService) StreamingOutputCall(_ context.Context, req *testpb.StreamingOutputCallRequest, send func(*testpb.StreamingOutputCallResponse) error) error {
	for _, p := range req.GetResponseParameters() {
		if err := send(&testpb.StreamingOutputCallResponse{Payload: &testpb.Payload{Body: make([]byte, p.GetSize())}}); err != nil {
			return err
		}
	}
	return nil
}

// StreamingInputCall sums the body sizes of the requests.
func (*testService) StreamingInputCall(_ context.Context, reqs iter.Seq2[*testpb.StreamingInputCallRequest, error]) (*testpb.StreamingInputCallResponse, error) {
	var total int32
	for req, err := range reqs {
		if err != nil {
			return nil, err
		}
		total += int32(len(req.GetPayload().GetBody()))
	}
	return &testpb.StreamingInputCallResponse{AggregatedPayloadSize: total}, nil
}

// FullDuplexCall answers every request with a body of the size of its first response parameter.
func (*testService) FullDuplexCall(_ context.Context, reqs iter.Seq2[*testpb.StreamingOutputCallRequest, error], send func(*testpb.StreamingOutputCallResponse) error) error {
	for req, err := range reqs {
		if err != nil {
			return err
		}
		var size int32
		if params := req.GetResponseParameters(); len(params) > 0 {
			size = params[0].GetSize()
		}
		if err := send(&testpb.StreamingOutputCallResponse{Payload: &testpb.Payload{Body: make([]byte, size)}}); err != nil {
			return err
		}
	}
	return nil
}

func newTestService() *testService {
	return &testService{}
}

// testServiceConfig serves the methods of testService.
func testServiceConfig() ServiceConfig {
	return ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{
			{Method: "UnaryCall", Handler: HandlerFor((*testService).UnaryCall)},
			{Method: "EmptyCall", Name: "empty", Handler: HandlerFor((*testService).EmptyCall)},
			{Method: "StreamingOutputCall", Handler: ServerStreamFor((*testService).StreamingOutputCall)},
			{Method: "StreamingInputCall", Handler: ClientStreamFor((*testService).StreamingInputCall)},
			{Method: "FullDuplexCall", Handler: BidiStreamFor((*testService).FullDuplexCall)},
		},
	}
}

// unaryConfig serves UnaryCall with h.
func unaryConfig(h Handler[*testpb.SimpleRequest, *testpb.SimpleResponse], middlewares ...forgo.MiddlewareFactory) ServiceConfig {
	return ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{
			Method:      "UnaryCall",
			Middlewares: middlewares,
			Handler: HandlerAs[*testpb.SimpleRequest, *testpb.SimpleResponse](func() Handler[*testpb.SimpleRequest, *testpb.SimpleResponse] {
				return h
			}),
		}},
	}
}

// syncBuffer is a bytes.Buffer that is safe for concurrent use, since calls log from grpc-go's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// harness is a server served on an in-memory listener, and a client connected to it.
type harness struct {
	s      *server
	logs   *syncBuffer
	conn   *grpc.ClientConn
	client testpb.TestServiceClient
}

// newServer builds and initializes a server through bootstrap, like App.Run does, with a logger that
// writes to logs.
func newServer(t testing.TB, cfg Config, logs io.Writer, providers ...any) *server {
	t.Helper()
	boot := forgo.NewBootstrap(nil, append([]any{newTestService}, providers...))
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })

	s, err := cfg.New(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(logs))); err != nil {
		t.Fatal(err)
	}
	return s.(*server)
}

// newHarness serves cfg on an in-memory listener.
func newHarness(t testing.TB, cfg Config, providers ...any) *harness {
	t.Helper()
	logs := &syncBuffer{}
	s := newServer(t, cfg, logs, providers...)

	lis := bufconn.Listen(1 << 20)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = s.g.Serve(lis)
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
	return &harness{s: s, logs: logs, conn: conn, client: testpb.NewTestServiceClient(conn)}
}

// lines returns the log lines, decoded.
func (h *harness) lines(t testing.TB) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for line := range strings.Lines(h.logs.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// line returns the last log line whose message is msg, waiting for one when there is none yet: grpc-go
// sends the status of a failed receive or send before the operation ends, so the client can get it before
// the line is logged.
func (h *harness) line(t testing.TB, msg string) map[string]any {
	t.Helper()
	return h.lineWhere(t, func(line map[string]any) bool { return line["message"] == msg })
}

// lineWhere returns the last log line match accepts, waiting up to 5 seconds for one.
func (h *harness) lineWhere(t testing.TB, match func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, line := range slices.Backward(h.lines(t)) {
			if match(line) {
				return line
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no such line in %s", h.logs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func testCtx(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestUnary(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})

	var trailer metadata.MD
	ctx := metadata.AppendToOutgoingContext(testCtx(t), ReqIDMetadata, "req-1")
	resp, err := h.client.UnaryCall(ctx, &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("hi")}}, grpc.Trailer(&trailer))
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetUsername() != "ada" || string(resp.GetPayload().GetBody()) != "hi" {
		t.Fatalf("got %v", resp)
	}
	if got := trailer.Get(ReqIDMetadata); !slices.Equal(got, []string{"req-1"}) {
		t.Fatalf("trailer %s = %v", ReqIDMetadata, got)
	}

	done := h.line(t, "operation completed")
	for key, want := range map[string]any{
		"op_type":   OperationTypeGRPCHandler,
		"op_name":   "/grpc.testing.TestService/UnaryCall",
		"op_id":     "req-1",
		"grpc_code": "OK",
	} {
		if done[key] != want {
			t.Errorf("%s = %v, want %v", key, done[key], want)
		}
	}
}

func TestOperationName(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	if _, err := h.client.EmptyCall(testCtx(t), &testpb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if name := h.line(t, "operation completed")["op_name"]; name != "empty" {
		t.Fatalf("op_name = %v", name)
	}
}

func TestOperationID(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	for _, tc := range []struct {
		name     string
		incoming []string
		keep     bool
	}{
		{"missing", nil, false},
		{"kept", []string{"abc-123"}, true},
		{"invalid", []string{"has space"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx(t)
			for _, id := range tc.incoming {
				ctx = metadata.AppendToOutgoingContext(ctx, ReqIDMetadata, id)
			}
			var trailer metadata.MD
			if _, err := h.client.EmptyCall(ctx, &testpb.Empty{}, grpc.Trailer(&trailer)); err != nil {
				t.Fatal(err)
			}
			got := trailer.Get(ReqIDMetadata)
			if len(got) != 1 || got[0] == "" {
				t.Fatalf("trailer %v", got)
			}
			if kept := tc.incoming != nil && got[0] == tc.incoming[0]; kept != tc.keep {
				t.Fatalf("id %q from %v", got[0], tc.incoming)
			}
		})
	}
}

// A call that a middleware rejects still gets its ID back.
func TestOperationIDOnRejection(t *testing.T) {
	deny := forgo.MiddlewareAs(func() operation.Middleware {
		return func(operation.Handler) operation.Handler {
			return func(*operation.Context) error {
				return operation.Error{Code: "denied", Status: operation.StatusPermissionDenied, Message: "denied"}
			}
		}
	})
	h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(newTestService().UnaryCall, deny)}})

	var trailer metadata.MD
	ctx := metadata.AppendToOutgoingContext(testCtx(t), ReqIDMetadata, "req-2")
	_, err := h.client.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Trailer(&trailer))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("got %v", err)
	}
	if got := trailer.Get(ReqIDMetadata); !slices.Equal(got, []string{"req-2"}) {
		t.Fatalf("trailer %v", got)
	}
}

func TestStreams(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	ctx := testCtx(t)

	t.Run("server", func(t *testing.T) {
		stream, err := h.client.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{
			ResponseParameters: []*testpb.ResponseParameters{{Size: 1}, {Size: 2}, {Size: 3}},
		})
		if err != nil {
			t.Fatal(err)
		}
		var sizes []int
		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			sizes = append(sizes, len(resp.GetPayload().GetBody()))
		}
		if !slices.Equal(sizes, []int{1, 2, 3}) {
			t.Fatalf("sizes %v", sizes)
		}
	})

	t.Run("client", func(t *testing.T) {
		stream, err := h.client.StreamingInputCall(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{4, 5} {
			if err := stream.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: make([]byte, n)}}); err != nil {
				t.Fatal(err)
			}
		}
		resp, err := stream.CloseAndRecv()
		if err != nil {
			t.Fatal(err)
		}
		if resp.GetAggregatedPayloadSize() != 9 {
			t.Fatalf("got %v", resp)
		}
		if body := h.line(t, "operation completed")["resp_body"]; !jsonEqual(body, `{"aggregated_payload_size":9}`) {
			t.Fatalf("resp_body %v", body)
		}
	})

	t.Run("bidi", func(t *testing.T) {
		stream, err := h.client.FullDuplexCall(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int32{7, 8} {
			req := &testpb.StreamingOutputCallRequest{ResponseParameters: []*testpb.ResponseParameters{{Size: n}}}
			if err := stream.Send(req); err != nil {
				t.Fatal(err)
			}
			resp, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			if got := int32(len(resp.GetPayload().GetBody())); got != n {
				t.Fatalf("got %d, want %d", got, n)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("got %v", err)
		}
	})
}

// jsonEqual reports whether the logged value v encodes to the JSON want.
func jsonEqual(v any, want string) bool {
	got, err := json.Marshal(v)
	if err != nil {
		return false
	}
	var a, b any
	_ = json.Unmarshal(got, &a)
	_ = json.Unmarshal([]byte(want), &b)
	ga, _ := json.Marshal(a)
	gb, _ := json.Marshal(b)
	return bytes.Equal(ga, gb)
}

// Stream handlers get the call's *operation.Context.
func TestStreamContext(t *testing.T) {
	type key struct{}
	got := make(chan *operation.Context, 1)
	svc := ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Args: map[any]any{key{}: "service"},
		Methods: []MethodConfig{{
			Method: "StreamingOutputCall",
			Handler: ServerStreamAs[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse](
				func() ServerStreamHandler[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse] {
					return func(ctx context.Context, _ *testpb.StreamingOutputCallRequest, _ func(*testpb.StreamingOutputCallResponse) error) error {
						op, _ := operation.FromCtx(ctx)
						got <- op
						return nil
					}
				}),
		}},
	}
	h := newHarness(t, Config{Services: []ServiceConfig{svc}})
	stream, err := h.client.StreamingOutputCall(testCtx(t), &testpb.StreamingOutputCallRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	op := <-got
	if op == nil || op.Type != OperationTypeGRPCHandler || op.Value(key{}) != "service" {
		t.Fatalf("got %+v", op)
	}
	if st := op.State.(*State); st.FullMethod != "/grpc.testing.TestService/StreamingOutputCall" || st.Request == nil {
		t.Fatalf("state %+v", st)
	}
}

// Chains run base, server, service, then method middlewares, and Args of a method override the service's.
func TestChainOrderAndArgs(t *testing.T) {
	type key struct{}
	var mu sync.Mutex
	var order []string
	record := func(name string) forgo.MiddlewareFactory {
		return forgo.MiddlewareAs(func() operation.Middleware {
			return func(next operation.Handler) operation.Handler {
				return func(ctx *operation.Context) error {
					mu.Lock()
					order = append(order, name)
					mu.Unlock()
					if name == "method" {
						// A write goes to this call's own copy of Args, so the next call never sees it.
						if _, leaked := ctx.Args.Get("written"); leaked {
							order = append(order, "leaked")
						}
						ctx.Args.Set("written", true)
					}
					return next(ctx)
				}
			}
		})
	}
	seen := make(chan [2]any, 2)
	svc := ServiceConfig{
		Desc:        &testpb.TestService_ServiceDesc,
		Middlewares: []forgo.MiddlewareFactory{record("service")},
		Args:        map[any]any{key{}: "service", "other": 1},
		Methods: []MethodConfig{{
			Method:      "EmptyCall",
			Middlewares: []forgo.MiddlewareFactory{record("method")},
			Args:        map[any]any{key{}: "method"},
			Handler: HandlerAs[*testpb.Empty, *testpb.Empty](func() Handler[*testpb.Empty, *testpb.Empty] {
				return func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
					seen <- [2]any{ctx.Value(key{}), ctx.Value("other")}
					return &testpb.Empty{}, nil
				}
			}),
		}},
	}
	cfg := Config{Services: []ServiceConfig{svc}}
	cfg.Middlewares = []forgo.MiddlewareFactory{record("server")}
	h := newHarness(t, cfg)

	for range 2 {
		if _, err := h.client.EmptyCall(testCtx(t), &testpb.Empty{}); err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(order, []string{"server", "service", "method", "server", "service", "method"}) {
		t.Fatalf("order %v", order)
	}
	if got := <-seen; got != [2]any{"method", 1} {
		t.Fatalf("args %v", got)
	}
	if len(svc.Args) != 2 || len(svc.Methods[0].Args) != 1 {
		t.Fatal("a write to Args reached the config")
	}
}

// Unary methods are registered as streams, and grpc-go still reports them as unary.
func TestServiceInfo(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	info := h.s.GetServiceInfo()["grpc.testing.TestService"]
	if info.Metadata != "grpc/testing/test.proto" {
		t.Fatalf("metadata %v", info.Metadata)
	}
	kinds := map[string][2]bool{}
	for _, m := range info.Methods {
		kinds[m.Name] = [2]bool{m.IsClientStream, m.IsServerStream}
	}
	want := map[string][2]bool{
		"UnaryCall":           {false, false},
		"EmptyCall":           {false, false},
		"StreamingOutputCall": {false, true},
		"StreamingInputCall":  {true, false},
		"FullDuplexCall":      {true, true},
	}
	if len(kinds) != len(want) {
		t.Fatalf("methods %v", kinds)
	}
	for name, k := range want {
		if kinds[name] != k {
			t.Errorf("%s: client, server streams = %v, want %v", name, kinds[name], k)
		}
	}
}

// Without base middlewares the server still answers errors, with the mapped status.
func TestDisableBaseMiddlewares(t *testing.T) {
	cfg := Config{Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		return nil, operation.Error{Code: "gone", Status: operation.StatusNotFound, Message: "gone"}
	})}}
	cfg.DisableBaseMiddlewares = true
	h := newHarness(t, cfg)

	var trailer metadata.MD
	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}, grpc.Trailer(&trailer))
	assertStatus(t, err, codes.NotFound, "gone", "gone")
	if h.logs.String() != "" {
		t.Fatalf("logged %s", h.logs)
	}
	if got := trailer.Get(ReqIDMetadata); got != nil {
		t.Fatalf("trailer %v without an ID", got)
	}

	// The unknown-method fallback too.
	_, err = testpb.NewUnimplementedServiceClient(h.conn).UnimplementedCall(testCtx(t), &testpb.Empty{})
	assertStatus(t, err, codes.Unimplemented, "unknown method /grpc.testing.UnimplementedService/UnimplementedCall", "unimplemented")
}

// statsRecorder records the status codes of the calls a stats.Handler is told about.
type statsRecorder struct {
	mu    sync.Mutex
	codes []codes.Code
}

func (r *statsRecorder) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (r *statsRecorder) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (r *statsRecorder) HandleConn(context.Context, stats.ConnStats) {}
func (r *statsRecorder) HandleRPC(_ context.Context, s stats.RPCStats) {
	if end, ok := s.(*stats.End); ok && !end.IsClient() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.codes = append(r.codes, status.Code(end.Error))
	}
}

// Stats handlers see the status every call ended with, including one grpc-go sent for a failed send.
func TestStatsHandlers(t *testing.T) {
	rec := &statsRecorder{}
	h := newHarness(t, Config{
		MaxSendMsgSize: 64,
		StatsHandlers:  []stats.Handler{rec, nil},
		Services: []ServiceConfig{unaryConfig(func(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			if req.GetResponseSize() < 0 {
				return nil, operation.Error{Code: "bad", Status: operation.StatusInvalidArgument, Message: "bad"}
			}
			return &testpb.SimpleResponse{Payload: &testpb.Payload{Body: make([]byte, req.GetResponseSize())}}, nil
		})},
	})
	for _, size := range []int32{1, -1, 128} {
		_, _ = h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{ResponseSize: size})
	}
	// grpc-go tells stats handlers that a call ended after the client may have its status, so the events of
	// consecutive calls can come in any order, and the last one later than the call returned.
	want := []codes.Code{codes.OK, codes.InvalidArgument, codes.ResourceExhausted}
	slices.Sort(want)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		rec.mu.Lock()
		got := slices.Sorted(slices.Values(rec.codes))
		rec.mu.Unlock()
		if slices.Equal(got, want) {
			return
		}
		if len(got) >= len(want) || time.Now().After(deadline) {
			t.Fatalf("codes %v, want %v", got, want)
		}
	}
}

// A middleware reads the metadata and the peer of a call from its State.
func TestStateMetadataAndPeer(t *testing.T) {
	type tenantKey struct{}
	tenant := forgo.MiddlewareAs(func() operation.Middleware {
		return func(next operation.Handler) operation.Handler {
			return func(ctx *operation.Context) error {
				st := ctx.State.(*State)
				p, ok := st.Peer()
				tenants := st.Metadata().Get("x-tenant")
				if !ok || p.Addr == nil || len(tenants) != 1 {
					return operation.Error{Code: "no_tenant", Status: operation.StatusUnauthenticated, Message: "no tenant"}
				}
				ctx.Args.Set(tenantKey{}, tenants[0])
				return next(ctx)
			}
		}
	})
	h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		return &testpb.SimpleResponse{Username: ctx.Value(tenantKey{}).(string)}, nil
	}, tenant)}})

	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	assertStatus(t, err, codes.Unauthenticated, "no tenant", "no_tenant")
	ctx := metadata.AppendToOutgoingContext(testCtx(t), "x-tenant", "acme")
	if resp, err := h.client.UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil || resp.GetUsername() != "acme" {
		t.Fatalf("got %v, %v", resp, err)
	}
}

func TestBidiStreamAs(t *testing.T) {
	svc := ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{
			Method: "FullDuplexCall",
			Handler: BidiStreamAs[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse](
				func(s *testService) BidiStreamHandler[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse] {
					return s.FullDuplexCall
				}),
		}},
	}
	h := newHarness(t, Config{Services: []ServiceConfig{svc}})
	stream, err := h.client.FullDuplexCall(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&testpb.StreamingOutputCallRequest{ResponseParameters: []*testpb.ResponseParameters{{Size: 2}}}); err != nil {
		t.Fatal(err)
	}
	if resp, err := stream.Recv(); err != nil || len(resp.GetPayload().GetBody()) != 2 {
		t.Fatalf("got %v, %v", resp, err)
	}
	_ = stream.CloseSend()
}

func TestServerNameAndTerm(t *testing.T) {
	cfg := Config{Services: []ServiceConfig{testServiceConfig()}}
	cfg.Term.TermTimeout = time.Minute
	s := newServer(t, cfg, io.Discard)
	if s.Name() != ServerNameGRPC || s.Term().TermTimeout != time.Minute {
		t.Fatalf("name %q, term %+v", s.Name(), s.Term())
	}
}
