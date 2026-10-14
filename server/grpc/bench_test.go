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

// BenchmarkUnary measures a unary call over an in-memory connection, client included: through forgo with
// and without the base middlewares and request logging, and through a bare grpc-go server.
func BenchmarkUnary(b *testing.B) {
	base := Config{SensitiveFields: []string{"body"}, Services: []ServiceConfig{testServiceConfig()}}
	noBase := base
	noBase.DisableBaseMiddlewares = true

	cases := []struct {
		name string
		g    func(b *testing.B) *grpc.Server
	}{
		{"base", func(b *testing.B) *grpc.Server { return newServer(b, base, io.Discard).g }},
		{"no_base", func(b *testing.B) *grpc.Server { return newServer(b, noBase, io.Discard).g }},
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
