package grpc

import (
	"context"
	"encoding/base64"
	"net"
	"strings"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestRequestLogging(t *testing.T) {
	h := newHarness(t, Config{
		SensitiveFields: []string{"body", "x-session"},
		Services:        []ServiceConfig{testServiceConfig()},
	})
	ctx := metadata.AppendToOutgoingContext(testCtx(t),
		"authorization", "Bearer secret-token",
		"x-session", "s3cr3t",
		"x-tenant", "acme",
		"trace-bin", string([]byte{0x00, 0xff, '\n'}),
	)
	req := &testpb.SimpleRequest{ResponseSize: 7, Payload: &testpb.Payload{Body: []byte("password")}}
	if _, err := h.client.UnaryCall(ctx, req); err != nil {
		t.Fatal(err)
	}

	started := h.line(t, "operation started")
	if started["path"] != "/grpc.testing.TestService/UnaryCall" || started["kind"] != "request" || started["addr"] == nil {
		t.Fatalf("logged %v", started)
	}
	headers, _ := started["req_headers"].(map[string]any)
	for key, want := range map[string]string{
		"authorization": "[*]",
		"x-session":     "[*]",
		"x-tenant":      "acme",
		"trace-bin":     base64.StdEncoding.EncodeToString([]byte{0x00, 0xff, '\n'}),
	} {
		if values, _ := headers[key].([]any); len(values) != 1 || values[0] != want {
			t.Errorf("req_headers[%s] = %v, want [%s]", key, headers[key], want)
		}
	}
	// Proto field names, a nested sensitive field masked, int32 kept as a number.
	if body := started["req_body"]; !jsonEqual(body, `{"response_size":7,"payload":{"body":"[*]"}}`) {
		t.Fatalf("req_body %v", body)
	}

	done := h.line(t, "operation completed")
	if done["kind"] != "response" || done["grpc_code"] != "OK" {
		t.Fatalf("logged %v", done)
	}
	if body := done["resp_body"]; !jsonEqual(body, `{"username":"ada","payload":{"body":"[*]"}}`) {
		t.Fatalf("resp_body %v", body)
	}
	if strings.Contains(h.logs.String(), "secret") || strings.Contains(h.logs.String(), base64.StdEncoding.EncodeToString([]byte("password"))) {
		t.Fatalf("a sensitive value reached the logs: %s", h.logs)
	}
}

// protojson adds random whitespace, so the server compacts what it logs.
func TestLoggedMessagesAreCompact(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	const calls = 20
	for range calls {
		req := &testpb.SimpleRequest{ResponseSize: 1, FillUsername: true, Payload: &testpb.Payload{Body: []byte("x")}}
		if _, err := h.client.UnaryCall(testCtx(t), req); err != nil {
			t.Fatal(err)
		}
	}
	want := `"req_body":{"response_size":1,"payload":{"body":"eA=="},"fill_username":true}`
	if got := strings.Count(h.logs.String(), want); got != calls {
		t.Fatalf("%d of %d compact bodies in %s", got, calls, h.logs)
	}
}

func TestLargeMessageNotLogged(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	req := &testpb.SimpleRequest{Payload: &testpb.Payload{Body: make([]byte, RequestRecorderBodyLimit+1)}}
	if _, err := h.client.UnaryCall(testCtx(t), req); err != nil {
		t.Fatal(err)
	}
	started := h.line(t, "operation started")
	if _, ok := started["req_body"]; ok || started["body_cut"] != true {
		t.Fatalf("logged %v", started)
	}
}

// A message under the limit on the wire whose JSON is over it is cut, and logged as a string.
func TestMessageCut(t *testing.T) {
	event := func(limit int, fields []string, m proto.Message) map[string]any {
		var logs syncBuffer
		lg := zerolog.New(&logs)
		e := lg.Info()
		logMessage(e, logger.RequestBodyLogKey, m, limit, newSensitiveFields(fields).fields)
		e.Msg("")
		return (&harness{logs: &logs}).lines(t)[0]
	}
	m := &testpb.SimpleRequest{Payload: &testpb.Payload{Body: []byte("abcdefgh")}}
	size := proto.Size(m)

	got := event(size+4, nil, m)
	if s, ok := got["req_body"].(string); !ok || got["body_cut"] != true || !strings.HasPrefix(s, `{"payload":`) {
		t.Fatalf("logged %v", got)
	}
	// With sensitive fields, the cut JSON is logged up to where it stops being valid.
	got = event(size+4, []string{"body"}, m)
	if s, _ := got["req_body"].(string); got["body_cut"] != true || strings.Contains(s, "YWJj") {
		t.Fatalf("logged %v", got)
	}
}

func TestRecorderDisabled(t *testing.T) {
	svc := testServiceConfig()
	svc.Args = map[any]any{ArgKeyRecorderDisabled: true}
	h := newHarness(t, Config{Services: []ServiceConfig{svc}})
	if _, err := h.client.EmptyCall(testCtx(t), &testpb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if logs := h.logs.String(); logs != "" {
		t.Fatalf("logged %s", logs)
	}
}

// The log fields of a call that has no Request, such as one that failed to receive it or the unknown-method
// fallback, need no message.
func TestLogFieldsWithoutMessages(t *testing.T) {
	var logs syncBuffer
	lg := zerolog.New(&logs)
	st := &State{FullMethod: "/a.B/C", stream: fakeStream{ctx: context.Background()}}
	op := &operation.Context{Context: context.Background(), State: st}

	e := lg.Info()
	RequestLogFields(1024, 1024, nil)(op, e)
	e.Msg("start")
	e = lg.Info()
	ResponseLogFields(1024, nil)(op, e)
	e.Msg("end")

	lines := (&harness{logs: &logs}).lines(t)
	if lines[0]["path"] != "/a.B/C" || lines[0]["req_headers"] != nil || lines[1]["grpc_code"] != "OK" {
		t.Fatalf("logged %v", lines)
	}
}

// fakeStream is a grpc.ServerStream that only has a context.
type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s fakeStream) Context() context.Context {
	return s.ctx
}

// appendAddr writes what String does.
func TestAppendAddr(t *testing.T) {
	for _, addr := range []net.Addr{
		&net.TCPAddr{IP: net.IPv4(10, 1, 2, 3).To4(), Port: 51234},
		&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 80}, // IPv4-mapped
		&net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443},
		&net.TCPAddr{IP: net.ParseIP("2001:db8:0:1:0:0:0:1"), Port: 0},
		&net.TCPAddr{IP: net.ParseIP("::ffff:0:1"), Port: 65535},
		&net.TCPAddr{IP: net.IPv6loopback, Port: 1, Zone: "lo0"},
		&net.TCPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 2, Zone: "eth0"},
		&net.TCPAddr{Port: 3},
		&net.TCPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 70000},
		&net.UnixAddr{Name: "/tmp/sock", Net: "unix"},
	} {
		if got, want := string(appendAddr(nil, addr)), addr.String(); got != want {
			t.Errorf("appendAddr(%#v) = %q, want %q", addr, got, want)
		}
	}
	if raceEnabled {
		return
	}
	addr := net.Addr(&net.TCPAddr{IP: net.ParseIP("10.1.2.3"), Port: 51234})
	var buf [64]byte
	if allocs := testing.AllocsPerRun(100, func() { appendAddr(buf[:0], addr) }); allocs != 0 {
		t.Fatalf("appendAddr allocated %v times", allocs)
	}
}
