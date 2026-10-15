package grpc

import (
	"context"
	"slices"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var errUnknownService = operation.Error{
	Code:    "unknown_service",
	Status:  operation.StatusNotFound,
	Message: "unknown service",
}

type (
	healthRequest  = healthpb.HealthCheckRequest
	healthResponse = healthpb.HealthCheckResponse
)

// HealthService serves the gRPC health checking protocol, grpc.health.v1.Health, for the server it is a
// service of: SERVING while the server runs, and NOT_SERVING once the server's Term.TermSig is signaled, so
// that Kubernetes probes and client-side load balancers stop sending calls during TermSig.Wait. Without a
// TermSig it is always SERVING.
//
// Check and Watch know the empty service name, for the whole server, and the name of every service of the
// server. Check answers NotFound for any other name, with errCode "unknown_service", and Watch sends
// SERVICE_UNKNOWN. Watch sends the current status, then NOT_SERVING when the TermSig is signaled, and ends
// when the client cancels or the server terminates. List returns the status of every known name.
//
// It is a ServiceConfig like any other, so its Args and Middlewares can be set, such as
// ArgKeyRecorderDisabled to keep frequent probes out of the logs.
func HealthService() ServiceConfig {
	return ServiceConfig{
		Desc: &healthpb.Health_ServiceDesc,
		Methods: []MethodConfig{
			{Method: "Check", Handler: unaryFactory(healthCheck)},
			{Method: "List", Handler: unaryFactory(healthList)},
			{Method: "Watch", Handler: serverStreamFactory(healthWatch)},
		},
	}
}

// servingStatus returns the status of the server.
func (e buildEnv) servingStatus() healthpb.HealthCheckResponse_ServingStatus {
	if e.termSig != nil && e.termSig.Signaled() {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}
	return healthpb.HealthCheckResponse_SERVING
}

// knows reports whether the health service knows service: the empty name, or a service of the server.
func (e buildEnv) knows(service string) bool {
	return service == "" || slices.Contains(e.services, service)
}

func healthCheck(_ forgo.Resolver, env buildEnv) (Handler[*healthRequest, *healthResponse], error) {
	return func(_ context.Context, req *healthRequest) (*healthResponse, error) {
		if !env.knows(req.GetService()) {
			return nil, errUnknownService
		}
		return &healthResponse{Status: env.servingStatus()}, nil
	}, nil
}

func healthList(_ forgo.Resolver, env buildEnv) (Handler[*healthpb.HealthListRequest, *healthpb.HealthListResponse], error) {
	return func(_ context.Context, _ *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
		status := env.servingStatus()
		statuses := make(map[string]*healthResponse, len(env.services)+1)
		statuses[""] = &healthResponse{Status: status}
		for _, service := range env.services {
			statuses[service] = &healthResponse{Status: status}
		}
		return &healthpb.HealthListResponse{Statuses: statuses}, nil
	}, nil
}

func healthWatch(_ forgo.Resolver, env buildEnv) (ServerStreamHandler[*healthRequest, *healthResponse], error) {
	return func(ctx context.Context, req *healthRequest, send func(*healthResponse) error) error {
		op, _ := operation.FromCtx(ctx)
		terminating := op.State.(*State).Terminating()

		status := healthpb.HealthCheckResponse_SERVICE_UNKNOWN
		if env.knows(req.GetService()) {
			status = env.servingStatus()
		}
		if err := send(&healthResponse{Status: status}); err != nil {
			return err
		}
		if status == healthpb.HealthCheckResponse_SERVING && env.termSig != nil {
			select {
			case <-env.termSig.Done():
				if err := send(&healthResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING}); err != nil {
					return err
				}
			case <-terminating:
				return nil
			case <-ctx.Done():
				return nil
			}
		}
		select {
		case <-terminating:
		case <-ctx.Done():
		}
		return nil
	}, nil
}
