package grpc

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// bareService serves UnaryCall the way grpc-go does without forgo, for comparison.
type bareService struct {
	testpb.UnimplementedTestServiceServer
}

func (bareService) UnaryCall(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return &testpb.SimpleResponse{Username: "ada", Payload: req.GetPayload()}, nil
}

// benchClient serves g on an in-memory listener and returns a client of it.
func benchClient(b *testing.B, g *grpc.Server) testpb.TestServiceClient {
	b.Helper()
	lis := bufconn.Listen(1 << 20)
	go func() { _ = g.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = conn.Close()
		g.Stop()
	})
	return testpb.NewTestServiceClient(conn)
}

// echoPayload, echoRequest and echoResponse are the business types of grpc.testing.TestService/UnaryCall.
type echoPayload struct {
	Type string
	Body []byte
}

type echoRequest struct {
	ResponseSize int
	Payload      echoPayload
	_            struct{} `grpc:"response_type"`
	_            struct{} `grpc:"fill_username"`
	_            struct{} `grpc:"fill_oauth_scope"`
	_            struct{} `grpc:"response_compressed"`
	_            struct{} `grpc:"response_status"`
	_            struct{} `grpc:"expect_compressed"`
	_            struct{} `grpc:"fill_server_id"`
	_            struct{} `grpc:"fill_grpclb_route_type"`
	_            struct{} `grpc:"orca_per_query_report"`
}

type echoResponse struct {
	Username string
	Payload  echoPayload
}

type echoService struct{}

func (echoService) Echo(_ context.Context, req echoRequest) (echoResponse, error) {
	return echoResponse{Username: "ada", Payload: req.Payload}, nil
}

// BenchmarkUnary measures a unary call over an in-memory connection, client included: through forgo with
// and without the base middlewares and request logging, with business structs converted from and to the
// messages, and through a bare grpc-go server.
func BenchmarkUnary(b *testing.B) {
	base := Config{SensitiveFields: []string{"body"}, Services: []ServiceConfig{testServiceConfig()}}
	noBase := base
	noBase.DisableBaseMiddlewares = true
	structs := Config{DisableBaseMiddlewares: true, Services: []ServiceConfig{{
		Desc:    &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{Method: "UnaryCall", Handler: HandlerFor(echoService.Echo)}},
	}}}

	cases := []struct {
		name string
		g    func(b *testing.B) *grpc.Server
	}{
		{"base", func(b *testing.B) *grpc.Server { return newServer(b, base, io.Discard).g }},
		{"no_base", func(b *testing.B) *grpc.Server { return newServer(b, noBase, io.Discard).g }},
		{"no_base_structs", func(b *testing.B) *grpc.Server {
			return newServer(b, structs, io.Discard, func() echoService { return echoService{} }).g
		}},
		{"grpc_go", func(*testing.B) *grpc.Server {
			g := grpc.NewServer()
			testpb.RegisterTestServiceServer(g, bareService{})
			return g
		}},
	}
	req := &testpb.SimpleRequest{ResponseSize: 4, Payload: &testpb.Payload{Body: []byte("payload")}}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			client := benchClient(b, c.g(b))
			ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer token")
			b.ReportAllocs()
			for b.Loop() {
				if _, err := client.UnaryCall(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkBinding measures the conversion of a request message to a business struct, and of a response
// struct to its message, without the network.
func BenchmarkBinding(b *testing.B) {
	req, err := newBinding[echoRequest]((&testpb.SimpleRequest{}).ProtoReflect().Descriptor(), true)
	if err != nil {
		b.Fatal(err)
	}
	resp, err := newBinding[echoResponse]((&testpb.SimpleResponse{}).ProtoReflect().Descriptor(), false)
	if err != nil {
		b.Fatal(err)
	}
	msg := &testpb.SimpleRequest{ResponseSize: 4, Payload: &testpb.Payload{Body: []byte("payload")}}
	ctx := context.Background()
	b.Run("request", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := req.toGo(ctx, msg); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("response", func(b *testing.B) {
		b.ReportAllocs()
		v := echoResponse{Username: "ada", Payload: echoPayload{Body: []byte("payload")}}
		for b.Loop() {
			if _, err := resp.toProto(v); err != nil {
				b.Fatal(err)
			}
		}
	})
}
