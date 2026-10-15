package grpc_test

import (
	"context"
	"os"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	fgrpc "github.com/efegreat/forgo/server/grpc"
	fhttp "github.com/efegreat/forgo/server/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// StatsService is business code: it imports no transport and no generated type.
type StatsService struct{}

func NewStatsService() *StatsService { return &StatsService{} }

// StatsRequest is bound from the query over HTTP, and from grpc.testing.LoadBalancerStatsRequest over gRPC,
// whose fields have the same names.
type StatsRequest struct {
	NumRPCs      int      `query:"num_rpcs" validate:"min=1"`
	TimeoutSec   int      `query:"timeout_sec"`
	MetadataKeys []string `query:"metadata_keys"`
}

// Stats is answered as JSON over HTTP, and as grpc.testing.LoadBalancerStatsResponse over gRPC.
type Stats struct {
	RPCsByPeer  map[string]int `json:"rpcs_by_peer"`
	NumFailures int            `json:"num_failures"`
}

func (s *StatsService) Collect(_ context.Context, req StatsRequest) (Stats, error) {
	if req.TimeoutSec > 60 {
		return Stats{}, operation.Error{Code: "timeout_too_long", Status: operation.StatusInvalidArgument, Message: "timeout over 60 seconds"}
	}
	return Stats{RPCsByPeer: map[string]int{"peer-1": req.NumRPCs}}, nil
}

func ExampleConfig() {
	termSig := &forgo.TermSig{}
	health := fgrpc.HealthService()
	health.Args = map[any]any{fgrpc.ArgKeyRecorderDisabled: true} // no log line per probe

	grpcServer := fgrpc.Config{
		Addr:         ":9090",
		ServerConfig: forgo.ServerConfig{Term: forgo.TermConfig{TermSig: termSig}},
		Reflection:   true,
		Services: []fgrpc.ServiceConfig{
			{
				// New checks the method name and streaming kind against the descriptor, and that StatsRequest
				// and Stats fit the method's messages: a mistake fails at startup.
				Desc: &testpb.LoadBalancerStatsService_ServiceDesc,
				Methods: []fgrpc.MethodConfig{
					{Method: "GetClientStats", Handler: fgrpc.HandlerFor((*StatsService).Collect)},
				},
			},
			health,
		},
	}
	// The very same method serves an HTTP route.
	httpServer := fhttp.Config{
		Addr: ":8080",
		Routes: []fhttp.RouteConfig{
			{Method: "GET", Path: "/stats", Handler: fhttp.HandlerFor((*StatsService).Collect)},
		},
	}

	app := forgo.NewApp(
		forgo.WithName("stats-api"),
		forgo.WithProviders(NewStatsService),
		forgo.WithServers(grpcServer, httpServer),
	)
	if err := app.Run(); err != nil {
		os.Exit(1)
	}
}

func ExampleUnaryInterceptor() {
	// A plain grpc-go interceptor runs as a middleware, inside the chain: the Recorder logs the calls it
	// rejects, and the Responder answers them.
	requireTenant := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if len(metadata.ValueFromIncomingContext(ctx, "x-tenant")) == 0 {
			return nil, status.Error(codes.Unauthenticated, "no tenant")
		}
		return handler(ctx, req)
	}

	conf := fgrpc.Config{
		ServerConfig: forgo.ServerConfig{
			Middlewares: []forgo.MiddlewareFactory{fgrpc.UnaryInterceptor(requireTenant)},
		},
		Services: []fgrpc.ServiceConfig{{
			Desc: &testpb.LoadBalancerStatsService_ServiceDesc,
			Methods: []fgrpc.MethodConfig{
				{Method: "GetClientStats", Handler: fgrpc.HandlerFor((*StatsService).Collect)},
			},
		}},
	}
	_ = conf
}
