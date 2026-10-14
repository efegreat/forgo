package grpc

import (
	"errors"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/efegreat/forgo"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflectionv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

func healthHarness(t *testing.T, ts *forgo.TermSig) (*harness, healthpb.HealthClient) {
	t.Helper()
	cfg := Config{Services: []ServiceConfig{testServiceConfig(), HealthService()}}
	cfg.Term.TermSig = ts
	h := newHarness(t, cfg)
	return h, healthpb.NewHealthClient(h.conn)
}

func TestHealthCheck(t *testing.T) {
	ts := &forgo.TermSig{}
	h, client := healthHarness(t, ts)

	check := func(service string) healthpb.HealthCheckResponse_ServingStatus {
		t.Helper()
		resp, err := client.Check(testCtx(t), &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetStatus()
	}
	for _, service := range []string{"", "grpc.testing.TestService", "grpc.health.v1.Health"} {
		if got := check(service); got != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("%q: %s", service, got)
		}
	}

	_, err := client.Check(testCtx(t), &healthpb.HealthCheckRequest{Service: "unknown.Service"})
	assertStatus(t, err, codes.NotFound, "unknown service", "unknown_service")

	ts.Signal()
	if got := check(""); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("after Signal: %s", got)
	}
	if name := h.line(t, "operation completed")["op_name"]; name != "/grpc.health.v1.Health/Check" {
		t.Fatalf("op_name %v", name)
	}
}

func TestHealthWithoutTermSig(t *testing.T) {
	_, client := healthHarness(t, nil)
	resp, err := client.Check(testCtx(t), &healthpb.HealthCheckRequest{})
	if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("got %v, %v", resp, err)
	}
}

func TestHealthList(t *testing.T) {
	ts := &forgo.TermSig{}
	_, client := healthHarness(t, ts)
	ts.Signal()
	resp, err := client.List(testCtx(t), &healthpb.HealthListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	names := slices.Sorted(maps.Keys(resp.GetStatuses()))
	if !slices.Equal(names, []string{"", "grpc.health.v1.Health", "grpc.testing.TestService"}) {
		t.Fatalf("names %q", names)
	}
	for name, st := range resp.GetStatuses() {
		if st.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
			t.Fatalf("%q: %s", name, st.GetStatus())
		}
	}
}

func TestHealthWatch(t *testing.T) {
	ts := &forgo.TermSig{}
	h, client := healthHarness(t, ts)

	stream, err := client.Watch(testCtx(t), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	recv := func() healthpb.HealthCheckResponse_ServingStatus {
		t.Helper()
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		return resp.GetStatus()
	}
	if got := recv(); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first: %s", got)
	}
	ts.Signal()
	if got := recv(); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("after Signal: %s", got)
	}

	// The watch ends when the server terminates, so it does not hold up the graceful stop.
	unknown, err := client.Watch(testCtx(t), &healthpb.HealthCheckRequest{Service: "unknown.Service"})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := unknown.Recv(); err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVICE_UNKNOWN {
		t.Fatalf("unknown: %v, %v", resp, err)
	}
	terminated := make(chan error, 1)
	go func() { terminated <- h.s.Terminate(testCtx(t)) }()
	for _, s := range []interface {
		Recv() (*healthpb.HealthCheckResponse, error)
	}{stream, unknown} {
		if _, err := s.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("got %v", err)
		}
	}
	select {
	case err := <-terminated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Terminate waits for the watch")
	}
}

func TestReflection(t *testing.T) {
	h := newHarness(t, Config{Reflection: true, Services: []ServiceConfig{testServiceConfig()}})

	t.Run("v1", func(t *testing.T) {
		stream, err := reflectionv1.NewServerReflectionClient(h.conn).ServerReflectionInfo(testCtx(t))
		if err != nil {
			t.Fatal(err)
		}
		req := &reflectionv1.ServerReflectionRequest{MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{}}
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, s := range resp.GetListServicesResponse().GetService() {
			names = append(names, s.GetName())
		}
		slices.Sort(names)
		want := []string{"grpc.reflection.v1.ServerReflection", "grpc.reflection.v1alpha.ServerReflection", "grpc.testing.TestService"}
		if !slices.Equal(names, want) {
			t.Fatalf("services %q", names)
		}

		req = &reflectionv1.ServerReflectionRequest{MessageRequest: &reflectionv1.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: "grpc.testing.TestService",
		}}
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
		if resp, err := stream.Recv(); err != nil || len(resp.GetFileDescriptorResponse().GetFileDescriptorProto()) == 0 {
			t.Fatalf("got %v, %v", resp, err)
		}
		_ = stream.CloseSend()
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("v1alpha", func(t *testing.T) {
		stream, err := reflectionv1alpha.NewServerReflectionClient(h.conn).ServerReflectionInfo(testCtx(t))
		if err != nil {
			t.Fatal(err)
		}
		req := &alphaRequest{MessageRequest: &reflectionv1alpha.ServerReflectionRequest_ListServices{}}
		if err := stream.Send(req); err != nil {
			t.Fatal(err)
		}
		resp, err := stream.Recv()
		//lint:ignore SA1019 the deprecated v1alpha API is what this test covers
		if err != nil || len(resp.GetListServicesResponse().GetService()) != 3 {
			t.Fatalf("got %v, %v", resp, err)
		}
		_ = stream.CloseSend()
	})

	// Reflection calls are operations.
	if name := h.line(t, "operation completed")["op_name"]; name != "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo" {
		t.Fatalf("op_name %v", name)
	}
}

// Only configured methods are served.
func TestUnconfiguredMethodOfKnownService(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{{
		Desc:    &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{Method: "EmptyCall", Handler: HandlerFor((*testService).EmptyCall)}},
	}}})
	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	if err == nil {
		t.Fatal("an unconfigured method was served")
	}
	assertStatus(t, err, codes.Unimplemented, "unknown method /grpc.testing.TestService/UnaryCall", "unimplemented")
}
