package grpc

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/typepb"
)

// bareService serves UnaryCall the way grpc-go does without forgo, for comparison.
type bareService struct {
	testpb.UnimplementedTestServiceServer
}

func (bareService) UnaryCall(_ context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return &testpb.SimpleResponse{Username: "ada", Payload: req.GetPayload()}, nil
}

// FullDuplexCall answers every request as testService does.
func (bareService) FullDuplexCall(stream testpb.TestService_FullDuplexCallServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		var size int32
		if params := req.GetResponseParameters(); len(params) > 0 {
			size = params[0].GetSize()
		}
		if err := stream.Send(&testpb.StreamingOutputCallResponse{Payload: &testpb.Payload{Body: make([]byte, size)}}); err != nil {
			return err
		}
	}
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
// and without the base middlewares and request logging, with a no-op operation.IObserver, with proto messages or business structs converted
// from and to them, and through a bare grpc-go server.
func BenchmarkUnary(b *testing.B) {
	base := Config{SensitiveFields: []string{"body"}, Services: []ServiceConfig{testServiceConfig()}}
	noBase := base
	noBase.DisableBaseMiddlewares = true
	structs := Config{DisableBaseMiddlewares: true, Services: []ServiceConfig{{
		Desc:    &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{Method: "UnaryCall", Handler: HandlerFor(echoService.Echo)}},
	}}}
	baseStructs := structs
	baseStructs.DisableBaseMiddlewares = false
	baseStructs.SensitiveFields = base.SensitiveFields

	cases := []struct {
		name string
		g    func(b *testing.B) *grpc.Server
	}{
		{"base", func(b *testing.B) *grpc.Server { return newServer(b, base, io.Discard).g }},
		{"base_observed", func(b *testing.B) *grpc.Server {
			return newServer(b, base, io.Discard, func() operation.IObserver { return nopObserver{} }).g
		}},
		{"no_base", func(b *testing.B) *grpc.Server { return newServer(b, noBase, io.Discard).g }},
		{"no_base_structs", func(b *testing.B) *grpc.Server {
			return newServer(b, structs, io.Discard, func() echoService { return echoService{} }).g
		}},
		{"base_structs", func(b *testing.B) *grpc.Server {
			return newServer(b, baseStructs, io.Discard, func() echoService { return echoService{} }).g
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

// BenchmarkBidiMessage measures one message each way on an open bidi stream, client included: through forgo
// with the base middlewares, with every stream message logged too, and through a bare grpc-go server.
func BenchmarkBidiMessage(b *testing.B) {
	base := Config{SensitiveFields: []string{"body"}, Services: []ServiceConfig{testServiceConfig()}}
	logged := base
	logged.LogStreamMessages = -1

	cases := []struct {
		name string
		g    func(b *testing.B) *grpc.Server
	}{
		{"base", func(b *testing.B) *grpc.Server { return newServer(b, base, io.Discard).g }},
		{"base_observed", func(b *testing.B) *grpc.Server {
			return newServer(b, base, io.Discard, func() operation.IObserver { return nopObserver{} }).g
		}},
		{"log_messages", func(b *testing.B) *grpc.Server { return newServer(b, logged, io.Discard).g }},
		{"grpc_go", func(*testing.B) *grpc.Server {
			g := grpc.NewServer()
			testpb.RegisterTestServiceServer(g, bareService{})
			return g
		}},
	}
	req := &testpb.StreamingOutputCallRequest{
		ResponseParameters: []*testpb.ResponseParameters{{Size: 4}},
		Payload:            &testpb.Payload{Body: []byte("payload")},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			stream, err := benchClient(b, c.g(b)).FullDuplexCall(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := stream.Send(req); err != nil {
					b.Fatal(err)
				}
				if _, err := stream.Recv(); err != nil {
					b.Fatal(err)
				}
			}
			if err := stream.CloseSend(); err != nil {
				b.Fatal(err)
			}
			if _, err := stream.Recv(); err != io.EOF {
				b.Fatalf("got %v, want the end of the stream", err)
			}
		})
	}
}

// fieldBusiness is a business struct of google.protobuf.Field, a generated message of ten fields.
type fieldBusiness struct {
	Kind         string
	Cardinality  string
	Number       int32
	Name         string
	TypeURL      string
	OneofIndex   int32
	Packed       bool
	JSONName     string
	DefaultValue string
	_            struct{} `grpc:"options"`
}

// BenchmarkBinding measures the conversion of a request message to a business struct, and of a response
// struct to its message, without the network: a small message, also converted by the functions of a Convert,
// and one of ten fields.
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

	// The same types, converted by functions of their own.
	conv := Convert(func(m *testpb.SimpleRequest) (echoRequest, error) {
		p := m.GetPayload()
		return echoRequest{ResponseSize: int(m.GetResponseSize()), Payload: echoPayload{Type: p.GetType().String(), Body: p.GetBody()}}, nil
	}, func(r echoResponse) (*testpb.SimpleResponse, error) {
		payload := &testpb.Payload{Type: testpb.PayloadType(testpb.PayloadType_value[r.Payload.Type]), Body: r.Payload.Body}
		return &testpb.SimpleResponse{Username: r.Username, Payload: payload}, nil
	})
	converted, err := convertedBindings[echoRequest, echoResponse](conv, methodMessages{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("convert_request", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := converted.req.toGo(ctx, msg); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("convert_response", func(b *testing.B) {
		b.ReportAllocs()
		v := echoResponse{Username: "ada", Payload: echoPayload{Body: []byte("payload")}}
		for b.Loop() {
			if _, err := converted.resp.toProto(v); err != nil {
				b.Fatal(err)
			}
		}
	})

	fd := (&typepb.Field{}).ProtoReflect().Descriptor()
	wideReq, err := newBinding[fieldBusiness](fd, true)
	if err != nil {
		b.Fatal(err)
	}
	wideResp, err := newBinding[fieldBusiness](fd, false)
	if err != nil {
		b.Fatal(err)
	}
	field := &typepb.Field{Kind: typepb.Field_TYPE_STRING, Cardinality: typepb.Field_CARDINALITY_OPTIONAL, Number: 7,
		Name: "display_name", TypeUrl: "type.googleapis.com/x", OneofIndex: 2, Packed: true, JsonName: "displayName",
		DefaultValue: "none"}
	b.Run("wide_request", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := wideReq.toGo(ctx, field); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("wide_response", func(b *testing.B) {
		b.ReportAllocs()
		v, err := wideReq.toGo(ctx, field)
		if err != nil {
			b.Fatal(err)
		}
		for b.Loop() {
			if _, err := wideResp.toProto(v); err != nil {
				b.Fatal(err)
			}
		}
	})
}
