package grpc_test

import (
	"context"
	"io"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	fgrpc "github.com/efegreat/forgo/server/grpc"

	"google.golang.org/grpc"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

// EchoService is business code. Its methods have the shapes of the generated service interface, but it
// does not have to implement that interface.
type EchoService struct{}

func NewEchoService() *EchoService { return &EchoService{} }

func (s *EchoService) Echo(ctx context.Context, req *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	if len(req.GetPayload().GetBody()) == 0 {
		return nil, operation.Error{Code: "empty_payload", Status: operation.StatusInvalidArgument, Message: "empty payload"}
	}
	return &testpb.SimpleResponse{Payload: req.GetPayload()}, nil
}

func (s *EchoService) Collect(stream grpc.ClientStreamingServer[testpb.StreamingInputCallRequest, testpb.StreamingInputCallResponse]) error {
	var total int32
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&testpb.StreamingInputCallResponse{AggregatedPayloadSize: total})
		}
		if err != nil {
			return err
		}
		total += int32(len(req.GetPayload().GetBody()))
	}
}

func ExampleConfig() {
	termSig := &forgo.TermSig{}
	health := fgrpc.HealthService()
	health.Args = map[any]any{fgrpc.ArgKeyRecorderDisabled: true} // no log line per probe

	conf := fgrpc.Config{
		Addr:         ":9090",
		ServerConfig: forgo.ServerConfig{Term: forgo.TermConfig{TermSig: termSig}},
		Reflection:   true,
		Services: []fgrpc.ServiceConfig{
			{
				// The handlers are checked against the descriptor in New: an unknown method, a wrong
				// streaming kind or a wrong message type fails at startup.
				Desc: &testpb.TestService_ServiceDesc,
				Methods: []fgrpc.MethodConfig{
					{Method: "UnaryCall", Handler: fgrpc.HandlerFor((*EchoService).Echo)},
					{Method: "StreamingInputCall", Handler: fgrpc.ClientStreamFor((*EchoService).Collect)},
				},
			},
			health,
		},
	}

	app := forgo.NewApp(
		forgo.WithName("echo-api"),
		forgo.WithProviders(NewEchoService),
		forgo.WithServers(conf),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}
