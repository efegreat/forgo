package grpc

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
	"testing"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	protoencoding "google.golang.org/grpc/encoding/proto"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// assertStatus checks the status of err, and the Reason of its errdetails.ErrorInfo, or that it has none
// when reason is empty.
func assertStatus(t testing.TB, err error, code codes.Code, msg, reason string) {
	t.Helper()
	s, ok := status.FromError(err)
	if !ok || s.Code() != code || s.Message() != msg {
		t.Fatalf("got %v, want %s %q", err, code, msg)
	}
	if got := errorInfo(s).GetReason(); got != reason {
		t.Fatalf("reason %q, want %q", got, reason)
	}
}

func errorInfo(s *status.Status) *errdetails.ErrorInfo {
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

func TestErrors(t *testing.T) {
	errSecret := errors.New("db password is hunter2")
	nameTaken := operation.Error{Code: "name_taken", Status: operation.StatusConflict, Message: "name taken"}
	withInfo, _ := status.New(codes.FailedPrecondition, "account locked").
		WithDetails(&errdetails.ErrorInfo{Reason: "ACCOUNT_LOCKED", Domain: "users.example.com"})

	for _, tc := range []struct {
		name     string
		handler  func() (*testpb.SimpleResponse, error)
		code     codes.Code
		msg      string
		reason   string
		errCode  string
		severity float64
		logged   string // in the logged error
	}{
		{
			name:    "operation error",
			handler: func() (*testpb.SimpleResponse, error) { return nil, nameTaken.Wrap(errSecret) },
			code:    codes.AlreadyExists, msg: "name taken", reason: "name_taken", errCode: "name_taken",
			logged: "name taken: db password is hunter2",
		},
		{
			name:    "wrapped operation error",
			handler: func() (*testpb.SimpleResponse, error) { return nil, fmt.Errorf("create user 7: %w", nameTaken) },
			code:    codes.AlreadyExists, msg: "name taken", reason: "name_taken", errCode: "name_taken",
			logged: "create user 7: name taken",
		},
		{
			name:    "unmapped error",
			handler: func() (*testpb.SimpleResponse, error) { return nil, errSecret },
			code:    codes.Internal, msg: "an error occurred", reason: "internal", errCode: "internal", severity: 2,
			logged: "an error occurred: db password is hunter2",
		},
		{
			name:    "status",
			handler: func() (*testpb.SimpleResponse, error) { return nil, status.Error(codes.NotFound, "user 7 not found") },
			code:    codes.NotFound, msg: "user 7 not found", errCode: "not_found",
			logged: "user 7 not found: code = NotFound",
		},
		{
			name: "wrapped status",
			handler: func() (*testpb.SimpleResponse, error) {
				return nil, fmt.Errorf("lookup: %w", status.Error(codes.OutOfRange, "page 9 out of range"))
			},
			code: codes.OutOfRange, msg: "page 9 out of range", errCode: "out_of_range",
			logged: "page 9 out of range: lookup: rpc error",
		},
		{
			name:    "status with error info",
			handler: func() (*testpb.SimpleResponse, error) { return nil, withInfo.Err() },
			code:    codes.FailedPrecondition, msg: "account locked", reason: "ACCOUNT_LOCKED", errCode: "ACCOUNT_LOCKED",
		},
		{
			name:    "internal status",
			handler: func() (*testpb.SimpleResponse, error) { return nil, status.Error(codes.Internal, "boom") },
			code:    codes.Internal, msg: "boom", errCode: "internal", severity: 2,
		},
		{
			name:    "canceled",
			handler: func() (*testpb.SimpleResponse, error) { return nil, fmt.Errorf("query: %w", context.Canceled) },
			code:    codes.Canceled, msg: "call canceled", reason: "canceled", errCode: "canceled",
		},
		{
			name:    "deadline",
			handler: func() (*testpb.SimpleResponse, error) { return nil, context.DeadlineExceeded },
			code:    codes.DeadlineExceeded, msg: "deadline exceeded", reason: "deadline_exceeded", errCode: "deadline_exceeded",
		},
		{
			name:    "panic",
			handler: func() (*testpb.SimpleResponse, error) { panic("boom") },
			code:    codes.Internal, msg: "an error occurred", reason: "internal", errCode: "internal", severity: 2,
			logged: "panic: boom",
		},
		{
			name:    "nil response",
			handler: func() (*testpb.SimpleResponse, error) { return nil, nil },
			code:    codes.Internal, msg: "handler returned no response", reason: "nil_response", errCode: "nil_response", severity: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				return tc.handler()
			})}})
			_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
			assertStatus(t, err, tc.code, tc.msg, tc.reason)

			failed := h.line(t, "operation failed")
			if failed["err_code"] != tc.errCode || failed["err_severity"] != tc.severity {
				t.Fatalf("err_code %v, err_severity %v", failed["err_code"], failed["err_severity"])
			}
			if failed["grpc_code"] != tc.code.String() {
				t.Fatalf("grpc_code %v", failed["grpc_code"])
			}
			if logged, _ := failed["error"].(string); !strings.Contains(logged, tc.logged) {
				t.Fatalf("logged %q, want it to hold %q", logged, tc.logged)
			}
		})
	}
}

func TestPanicStackLogged(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
		panic(errors.New("nil map"))
	})}})
	_, _ = h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	if logged, _ := h.line(t, "operation failed")["error"].(string); !strings.Contains(logged, "goroutine") {
		t.Fatalf("no stack in %q", logged)
	}
}

func TestErrorDomain(t *testing.T) {
	h := newHarness(t, Config{
		ErrorDomain: "users.example.com",
		Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return nil, operation.Error{Code: "gone", Status: operation.StatusNotFound, Message: "gone"}
		})},
	})
	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	if info := errorInfo(status.Convert(err)); info.GetDomain() != "users.example.com" || info.GetReason() != "gone" {
		t.Fatalf("got %v", info)
	}
}

func TestCustomErrorMapper(t *testing.T) {
	errDB := errors.New("db down")
	h := newHarness(t, Config{
		ErrorMapper: operation.NewErrorMapper(func(err error) (operation.Error, bool) {
			if errors.Is(err, errDB) {
				return operation.Error{Code: "db", Status: operation.StatusUnavailable, Message: "try again"}, true
			}
			return operation.Error{}, false
		}),
		Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return nil, errDB
		})},
	})
	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	assertStatus(t, err, codes.Unavailable, "try again", "db")
	// The mapper left the error out, so the server wraps it for the log.
	if logged := h.line(t, "operation failed")["error"]; logged != "try again: db down" {
		t.Fatalf("logged %v", logged)
	}
}

func TestUnknownMethod(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	for _, method := range []string{
		"/grpc.testing.UnimplementedService/UnimplementedCall", // unknown service
		"/grpc.testing.TestService/UnimplementedCall",          // a method of the service that is not configured
	} {
		err := h.conn.Invoke(testCtx(t), method, &testpb.Empty{}, &testpb.Empty{})
		assertStatus(t, err, codes.Unimplemented, "unknown method "+method, "unimplemented")
		failed := h.line(t, "operation failed")
		if failed["op_name"] != "unimplemented" || failed["err_severity"] != 0.0 {
			t.Fatalf("logged %v", failed)
		}
		if path := h.line(t, "operation started")["path"]; path != method {
			t.Fatalf("path %v", path)
		}
	}
}

// validatedRequest is a SimpleRequest with the Validate method protoc-gen-validate generates.
type validatedRequest struct {
	testpb.SimpleRequest
}

func (r *validatedRequest) Validate() error {
	if r.GetResponseSize() < 0 {
		return errors.New("response_size must not be negative")
	}
	return nil
}

func TestValidation(t *testing.T) {
	called := false
	svc := ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{
			Method: "UnaryCall",
			Handler: HandlerAs[*validatedRequest, *testpb.SimpleResponse](func() Handler[*validatedRequest, *testpb.SimpleResponse] {
				return func(context.Context, *validatedRequest) (*testpb.SimpleResponse, error) {
					called = true
					return &testpb.SimpleResponse{}, nil
				}
			}),
		}},
	}

	t.Run("Validate method", func(t *testing.T) {
		h := newHarness(t, Config{Services: []ServiceConfig{svc}})
		_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{ResponseSize: -1})
		assertStatus(t, err, codes.InvalidArgument, "response_size must not be negative", "validation")
		if called {
			t.Fatal("the handler ran")
		}
		if logged := h.line(t, "operation failed")["error"]; logged != "response_size must not be negative: invalid grpc.testing.SimpleRequest" {
			t.Fatalf("logged %v", logged)
		}
		if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{ResponseSize: 1}); err != nil || !called {
			t.Fatalf("valid request: %v", err)
		}
	})

	t.Run("custom validator", func(t *testing.T) {
		called = false
		h := newHarness(t, Config{
			Validator: func(any) error { return nil },
			Services:  []ServiceConfig{svc},
		})
		if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{ResponseSize: -1}); err != nil || !called {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("server stream request", func(t *testing.T) {
		h := newHarness(t, Config{
			Validator: func(m any) error {
				if req, ok := m.(*testpb.StreamingOutputCallRequest); ok && len(req.GetResponseParameters()) > 2 {
					return errors.New("too many responses")
				}
				return nil
			},
			Services: []ServiceConfig{testServiceConfig()},
		})
		stream, err := h.client.StreamingOutputCall(testCtx(t), &testpb.StreamingOutputCallRequest{
			ResponseParameters: []*testpb.ResponseParameters{{}, {}, {}},
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = stream.Recv()
		assertStatus(t, err, codes.InvalidArgument, "too many responses", "validation")
	})

	t.Run("streamed messages", func(t *testing.T) {
		h := newHarness(t, Config{
			Validator: func(m any) error {
				if req, ok := m.(*testpb.StreamingInputCallRequest); ok && len(req.GetPayload().GetBody()) > 3 {
					return errors.New("body too long")
				}
				return nil
			},
			Services: []ServiceConfig{testServiceConfig()},
		})
		stream, err := h.client.StreamingInputCall(testCtx(t))
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("ok")}})
		_ = stream.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("too long")}})
		_, err = stream.CloseAndRecv()
		assertStatus(t, err, codes.InvalidArgument, "body too long", "validation")
	})
}

func TestMessageTooLarge(t *testing.T) {
	called := false
	h := newHarness(t, Config{
		MaxRecvMsgSize: 1024,
		Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			called = true
			return &testpb.SimpleResponse{}, nil
		})},
	})
	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{Payload: &testpb.Payload{Body: make([]byte, 2048)}})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}
	if called {
		t.Fatal("the handler ran")
	}
	failed := h.line(t, "operation failed")
	if failed["err_code"] != "message_too_large" || failed["grpc_code"] != "ResourceExhausted" || failed["err_severity"] != 0.0 {
		t.Fatalf("logged %v", failed)
	}
	if _, ok := h.line(t, "operation started")["req_body"]; ok {
		t.Fatal("logged a request that was not received")
	}
}

// rawCodec sends bytes as they are, under the name of the proto codec.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return v.([]byte), nil }
func (rawCodec) Unmarshal(data []byte, v any) error {
	*v.(*[]byte) = append([]byte(nil), data...)
	return nil
}
func (rawCodec) Name() string { return "proto" }

// A message that cannot be decoded is the client's error: the server answers it with InvalidArgument, protobuf's
// reason and an ErrorInfo, where grpc-go alone would answer Internal. The handler of a stream gets it from the
// requests, after the messages before it.
func TestInvalidMessage(t *testing.T) {
	invalid := []byte{0xff, 0xff, 0xff} // a truncated tag
	valid, err := proto.Marshal(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte("hi")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method   string
		messages [][]byte
	}{
		{"UnaryCall", [][]byte{invalid}},
		{"StreamingOutputCall", [][]byte{invalid}},
		{"StreamingInputCall", [][]byte{valid, invalid}},
		{"FullDuplexCall", [][]byte{invalid}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
			desc := &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}
			stream, err := h.conn.NewStream(testCtx(t), desc, "/grpc.testing.TestService/"+tc.method, grpc.ForceCodec(rawCodec{}))
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range tc.messages {
				if err := stream.SendMsg(m); err != nil {
					t.Fatal(err)
				}
			}
			_ = stream.CloseSend()
			var out []byte
			for err = stream.RecvMsg(&out); err == nil; err = stream.RecvMsg(&out) {
			}
			assertUndecodable(t, err, "cannot parse invalid wire-format data")

			failed := h.line(t, "operation failed")
			if failed["err_code"] != "invalid_message" || failed["grpc_code"] != "InvalidArgument" || failed["err_severity"] != 0.0 {
				t.Fatalf("logged %v", failed)
			}
			if failed["error"] != status.Convert(err).Message() {
				t.Fatalf("logged error %v", failed["error"])
			}
		})
	}

	// The reason names the field, of a message that is not generated code too.
	t.Run("invalid UTF-8", func(t *testing.T) {
		h := newHarness(t, Config{Services: []ServiceConfig{usersConfig()}}, newUserService)
		var out []byte
		err := h.conn.Invoke(testCtx(t), "/forgotest.Users/Get", []byte{0x0a, 0x01, 0xff}, &out, grpc.ForceCodec(rawCodec{}))
		assertUndecodable(t, err, "field forgotest.GetUserRequest.id contains invalid UTF-8")
	})
}

// assertUndecodable checks that err is the status of a message that cannot be decoded for reason. protobuf
// writes the space of its "proto:" prefix either way, so the message is matched around it.
func assertUndecodable(t testing.TB, err error, reason string) {
	t.Helper()
	s := status.Convert(err)
	if s.Code() != codes.InvalidArgument || !strings.HasPrefix(s.Message(), "invalid request message: proto:") ||
		!strings.HasSuffix(s.Message(), reason) || errorInfo(s).GetReason() != "invalid_message" {
		t.Fatalf("got %v, want the reason %q", err, reason)
	}
}

// The codec is the one registered for protobuf, which never fails to decode into a decodeTarget.
func TestCodec(t *testing.T) {
	c := newCodec()
	if c.CodecV2 != encoding.GetCodecV2(protoencoding.Name) || c.Name() != protoencoding.Name {
		t.Fatalf("codec %v, %q", c.CodecV2, c.Name())
	}
	data, err := c.Marshal(&testpb.Payload{Body: []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	target := decodeTarget{msg: &testpb.Payload{}}
	if err := c.Unmarshal(data, &target); err != nil || target.err != nil || string(target.msg.(*testpb.Payload).GetBody()) != "hi" {
		t.Fatalf("decoded %v, %v, %v", target.msg, err, target.err)
	}
	invalid := mem.BufferSlice{mem.SliceBuffer{0xff, 0xff, 0xff}}
	if err := c.Unmarshal(invalid, &target); err != nil || target.err == nil {
		t.Fatalf("got %v, %v", err, target.err)
	}
	if err := c.Unmarshal(invalid, &testpb.Payload{}); err == nil {
		t.Fatal("decoded an invalid message")
	}
}

// payloadRecorder records the request messages stats handlers are told about.
type payloadRecorder struct {
	statsRecorder
	payloads []proto.Message
}

func (r *payloadRecorder) HandleRPC(_ context.Context, s stats.RPCStats) {
	if in, ok := s.(*stats.InPayload); ok && !in.IsClient() {
		m, _ := in.Payload.(proto.Message)
		r.mu.Lock()
		defer r.mu.Unlock()
		if m == nil {
			r.payloads = append(r.payloads, nil)
			return
		}
		r.payloads = append(r.payloads, proto.Clone(m))
	}
}

// The server receives into a message of its own, which stats handlers read like the request.
func TestStatsHandlerPayload(t *testing.T) {
	rec := &payloadRecorder{}
	h := newHarness(t, Config{StatsHandlers: []stats.Handler{rec}, Services: []ServiceConfig{testServiceConfig()}})
	req := &testpb.SimpleRequest{ResponseSize: 3, Payload: &testpb.Payload{Body: []byte("hi")}}
	if _, err := h.client.UnaryCall(testCtx(t), req); err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.payloads) != 1 || !proto.Equal(rec.payloads[0], req) {
		t.Fatalf("payloads %v, want %v", rec.payloads, req)
	}
}

// A unary method registered as a stream still takes exactly one request.
func TestUnaryCardinality(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{testServiceConfig()}})
	stream, err := h.conn.NewStream(testCtx(t), &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/grpc.testing.TestService/UnaryCall")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := stream.SendMsg(&testpb.SimpleRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	_ = stream.CloseSend()
	if err := stream.RecvMsg(&testpb.SimpleResponse{}); status.Code(err) != codes.Internal {
		t.Fatalf("got %v", err)
	}
	if code := h.line(t, "operation failed")["err_code"]; code != "invalid_message" {
		t.Fatalf("err_code %v", code)
	}
}

// The response is sent inside the chain, so a send that fails is logged as a failure. With grpc-go's
// MethodDesc path the response would be sent after the operation completed.
func TestSendFailed(t *testing.T) {
	h := newHarness(t, Config{
		MaxSendMsgSize: 64,
		Services: []ServiceConfig{unaryConfig(func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return &testpb.SimpleResponse{Payload: &testpb.Payload{Body: make([]byte, 128)}}, nil
		})},
	})
	_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}
	failed := h.line(t, "operation failed")
	if failed["err_code"] != "send_failed" || failed["grpc_code"] != "ResourceExhausted" || failed["err_severity"] != 1.0 {
		t.Fatalf("logged %v", failed)
	}
}

// A stream handler that ignores a failed send still fails its operation, with the status grpc-go sent.
func TestStreamFailureIgnored(t *testing.T) {
	svc := ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{
			Method: "StreamingOutputCall",
			Handler: ServerStreamAs[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse](
				func() ServerStreamHandler[*testpb.StreamingOutputCallRequest, *testpb.StreamingOutputCallResponse] {
					return func(_ context.Context, _ *testpb.StreamingOutputCallRequest, send func(*testpb.StreamingOutputCallResponse) error) error {
						_ = send(&testpb.StreamingOutputCallResponse{Payload: &testpb.Payload{Body: make([]byte, 128)}})
						return nil
					}
				}),
		}},
	}
	h := newHarness(t, Config{MaxSendMsgSize: 64, Services: []ServiceConfig{svc}})
	stream, err := h.client.StreamingOutputCall(testCtx(t), &testpb.StreamingOutputCallRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}
	failed := h.line(t, "operation failed")
	if failed["err_code"] != "send_failed" || failed["grpc_code"] != "ResourceExhausted" {
		t.Fatalf("logged %v", failed)
	}
}

// A client stream whose handler returns the error of a failed receive is logged with the code grpc-go sent,
// and keeps the handler's text.
func TestStreamReceiveFailed(t *testing.T) {
	svc := ServiceConfig{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{
			Method: "StreamingInputCall",
			Handler: ClientStreamAs[*testpb.StreamingInputCallRequest, *testpb.StreamingInputCallResponse](
				func() ClientStreamHandler[*testpb.StreamingInputCallRequest, *testpb.StreamingInputCallResponse] {
					return func(_ context.Context, reqs iter.Seq2[*testpb.StreamingInputCallRequest, error]) (*testpb.StreamingInputCallResponse, error) {
						for _, err := range reqs {
							if err != nil {
								return nil, fmt.Errorf("import: %w", err)
							}
						}
						return &testpb.StreamingInputCallResponse{}, nil
					}
				}),
		}},
	}
	h := newHarness(t, Config{MaxRecvMsgSize: 1024, Services: []ServiceConfig{svc}})
	stream, err := h.client.StreamingInputCall(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: make([]byte, 10)}})
	_ = stream.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: make([]byte, 2048)}})
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("got %v", err)
	}
	failed := h.line(t, "operation failed")
	if logged, _ := failed["error"].(string); failed["err_code"] != "message_too_large" || !strings.HasPrefix(logged, "request message too large: import: ") {
		t.Fatalf("logged %v", failed)
	}
}

func TestMapStatusIgnoresOK(t *testing.T) {
	if _, ok := MapStatus(status.Error(codes.OK, "")); ok {
		t.Fatal("mapped an OK status")
	}
	if _, ok := MapStatus(errors.New("plain")); ok {
		t.Fatal("mapped a plain error")
	}
}

func TestStatusMaps(t *testing.T) {
	for st, code := range grpcStatusMap {
		if st == operation.StatusRateLimited || st == operation.StatusTooLarge {
			continue // both are ResourceExhausted
		}
		if back := operationStatus(code); back != st && !(st == operation.StatusUnknown) {
			t.Errorf("%d → %s → %d", st, code, back)
		}
	}
	if got := operationStatus(codes.Unknown); got != operation.StatusUnknown {
		t.Fatalf("Unknown → %d", got)
	}
	if got := codeNames[codes.FailedPrecondition]; got != "failed_precondition" {
		t.Fatalf("name %q", got)
	}
}

type (
	untaggedRequest struct {
		Name string
		Tags []string
		Next *untaggedRequest
	}
	taggedRequest struct {
		Name string `validate:"required"`
	}
	valueValidated   struct{ Name string }
	pointerValidated struct{ Name string }
)

func (valueValidated) Validate() error    { return nil }
func (*pointerValidated) Validate() error { return nil }

// The default Validator is skipped on request types it has nothing to check on.
func TestDefaultValidatorValidates(t *testing.T) {
	_, validates, err := newDefaultValidator(nil)
	if err != nil {
		t.Fatal(err)
	}
	type embedded struct{ taggedRequest }
	for _, tc := range []struct {
		t    reflect.Type
		want bool
	}{
		{reflect.TypeFor[untaggedRequest](), false},
		{reflect.TypeFor[*untaggedRequest](), false},
		{reflect.TypeFor[*testpb.SimpleRequest](), false},
		{reflect.TypeFor[taggedRequest](), true},
		{reflect.TypeFor[struct{ Inner taggedRequest }](), true},
		{reflect.TypeFor[struct{ Items []*taggedRequest }](), true},
		{reflect.TypeFor[struct{ ByKey map[taggedRequest]int }](), true},
		{reflect.TypeFor[struct{ ByValue map[string]taggedRequest }](), true},
		{reflect.TypeFor[embedded](), true},
		{reflect.TypeFor[struct {
			Plain untaggedRequest
			N     int `validate:"min=1"`
		}](), true},
		{reflect.TypeFor[valueValidated](), true},
		{reflect.TypeFor[pointerValidated](), true},
		{reflect.TypeFor[*pointerValidated](), true},
		{reflect.TypeFor[*validatedRequest](), true},
	} {
		if got := validates(tc.t); got != tc.want {
			t.Errorf("validates(%s) = %v, want %v", tc.t, got, tc.want)
		}
	}
}
