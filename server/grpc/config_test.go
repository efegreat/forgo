package grpc

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

// unregisteredDesc is a hand-written descriptor whose service is not in the proto registry.
var unregisteredDesc = grpc.ServiceDesc{
	ServiceName: "example.Unregistered",
	Methods:     []grpc.MethodDesc{{MethodName: "Get"}},
}

// notProto is a request type that is not a proto message.
type notProto struct{}

func TestNewErrors(t *testing.T) {
	errFactory := errors.New("factory failed")
	failing := func(forgo.Resolver) (operation.Middleware, error) { return nil, errFactory }
	unary := HandlerFor((*testService).UnaryCall)

	for _, tc := range []struct {
		name     string
		services []ServiceConfig
		want     string
	}{
		{"nil Desc", []ServiceConfig{{}}, "grpc: service 0: no Desc"},
		{
			"duplicate service",
			[]ServiceConfig{testServiceConfig(), testServiceConfig()},
			`grpc: service "grpc.testing.TestService": configured twice`,
		},
		{"no methods", []ServiceConfig{{Desc: &testpb.TestService_ServiceDesc}}, `service "grpc.testing.TestService": no Methods`},
		{
			"duplicate method",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{
				{Method: "UnaryCall", Handler: unary}, {Method: "UnaryCall", Handler: unary},
			}}},
			`grpc: service "grpc.testing.TestService" method "UnaryCall": configured twice`,
		},
		{
			"no method name",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{{Handler: unary}}}},
			`method "": no Method`,
		},
		{
			"no handler",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{{Method: "UnaryCall"}}}},
			`method "UnaryCall": no Handler`,
		},
		{
			"unknown method",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{{Method: "Unary", Handler: unary}}}},
			`method "Unary": the service has no such method`,
		},
		{
			"unary handler for a stream",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{{Method: "StreamingOutputCall", Handler: unary}}}},
			"the method is server-streaming, but the handler is unary",
		},
		{
			"stream handler for a unary method",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{
				{Method: "UnaryCall", Handler: BidiStreamFor((*testService).FullDuplexCall)},
			}}},
			"the method is unary, but the handler is bidi-streaming",
		},
		{
			"wrong stream kind",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{
				{Method: "StreamingInputCall", Handler: BidiStreamFor((*testService).FullDuplexCall)},
			}}},
			"the method is client-streaming, but the handler is bidi-streaming",
		},
		{
			"wrong request type",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{
				{Method: "EmptyCall", Handler: unary},
			}}},
			"request type: *grpc_testing.SimpleRequest is grpc.testing.SimpleRequest, but the method's is grpc.testing.Empty",
		},
		{
			"wrong response type",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{
				{Method: "UnaryCall", Handler: HandlerAs[*testpb.SimpleRequest, *testpb.Empty](func() Handler[*testpb.SimpleRequest, *testpb.Empty] { return nil })},
			}}},
			"response type: *grpc_testing.Empty is grpc.testing.Empty, but the method's is grpc.testing.SimpleResponse",
		},
		{
			"not a proto message",
			[]ServiceConfig{{Desc: &unregisteredDesc, Methods: []MethodConfig{
				{Method: "Get", Handler: HandlerAs[notProto, *testpb.Empty](func() Handler[notProto, *testpb.Empty] { return nil })},
			}}},
			`grpc: service "example.Unregistered" method "Get": request type: grpc.notProto is not a proto message, and the service's descriptor is not in the proto registry`,
		},
		{
			"missing dependency",
			[]ServiceConfig{{Desc: &healthpb.Health_ServiceDesc, Methods: []MethodConfig{
				{Method: "Check", Handler: HandlerFor(healthpb.HealthServer.Check)},
			}}},
			`grpc: service "grpc.health.v1.Health" method "Check": `,
		},
		{
			"failing service middleware",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Middlewares: []forgo.MiddlewareFactory{failing},
				Methods: []MethodConfig{{Method: "UnaryCall", Handler: unary}}}},
			`grpc: service "grpc.testing.TestService": factory failed`,
		},
		{
			"failing method middleware",
			[]ServiceConfig{{Desc: &testpb.TestService_ServiceDesc, Methods: []MethodConfig{
				{Method: "UnaryCall", Handler: unary, Middlewares: []forgo.MiddlewareFactory{failing}},
			}}},
			`grpc: service "grpc.testing.TestService" method "UnaryCall": factory failed`,
		},
		{
			"reflection configured twice",
			[]ServiceConfig{reflectionServices(nil)[0]},
			`grpc: service "grpc.reflection.v1.ServerReflection": configured twice`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			boot := forgo.NewBootstrap(nil, []any{newTestService})
			r, err := boot.Build()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = boot.Close() }()

			_, err = Config{Reflection: true, Services: tc.services}.New(r)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNewServerMiddlewareError(t *testing.T) {
	errFactory := errors.New("factory failed")
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = boot.Close() }()
	cfg := Config{Services: []ServiceConfig{testServiceConfig()}}
	cfg.Middlewares = []forgo.MiddlewareFactory{func(forgo.Resolver) (operation.Middleware, error) { return nil, errFactory }}
	if _, err := cfg.New(r); !errors.Is(err, errFactory) {
		t.Fatalf("got %v", err)
	}
}

// A service whose descriptor is not in the proto registry is served with proto messages of any type.
func TestUnregisteredService(t *testing.T) {
	desc := grpc.ServiceDesc{
		ServiceName: "example.Echo",
		Methods:     []grpc.MethodDesc{{MethodName: "Echo"}},
	}
	h := newHarness(t, Config{Services: []ServiceConfig{{
		Desc: &desc,
		Methods: []MethodConfig{{
			Method: "Echo",
			Handler: HandlerAs[*testpb.SimpleRequest, *testpb.SimpleResponse](func() Handler[*testpb.SimpleRequest, *testpb.SimpleResponse] {
				return func(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
					return &testpb.SimpleResponse{Payload: req.GetPayload()}, nil
				}
			}),
		}},
	}}})
	var resp testpb.SimpleResponse
	req := &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("echo")}}
	if err := h.conn.Invoke(testCtx(t), "/example.Echo/Echo", req, &resp); err != nil {
		t.Fatal(err)
	}
	if string(resp.GetPayload().GetBody()) != "echo" {
		t.Fatalf("got %v", &resp)
	}
}

func TestSanitize(t *testing.T) {
	c := Config{}.Sanitize()
	if c.Network != "tcp" || c.Addr != ":9090" || c.MaxRecvMsgSize != 4<<20 || c.MaxSendMsgSize != math.MaxInt32 {
		t.Fatalf("got %+v", c)
	}
	if c.ErrorMapper == nil || c.Term.TermTimeout <= 0 {
		t.Fatal("missing defaults")
	}
	if c.Validator != nil {
		t.Fatal("Sanitize built the Validator, which New builds")
	}
	c = Config{MaxRecvMsgSize: -1, MaxSendMsgSize: -1}.Sanitize()
	if c.MaxRecvMsgSize != math.MaxInt32 || c.MaxSendMsgSize != math.MaxInt32 {
		t.Fatalf("negative sizes: %d, %d", c.MaxRecvMsgSize, c.MaxSendMsgSize)
	}
	if (Config{}).Name() != ServerNameGRPC {
		t.Fatal("name")
	}
}
