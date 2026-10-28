package grpc

import (
	"context"
	"errors"
	"io"
	"iter"
	"strings"
	"testing"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
)

// echo is the business type of grpc.testing.SimpleRequest and SimpleResponse in the tests of Convert, which
// share no field name with them.
type echo struct {
	Size int `validate:"min=0"`
	Text string
}

// other is a business type no conversion converts.
type other struct{}

func echoOf(m *testpb.SimpleRequest) (echo, error) {
	return echo{Size: int(m.GetResponseSize()), Text: string(m.GetPayload().GetBody())}, nil
}

func echoMessage(e echo) (*testpb.SimpleResponse, error) {
	return &testpb.SimpleResponse{Username: e.Text, Payload: &testpb.Payload{Body: make([]byte, e.Size)}}, nil
}

// shout serves UnaryCall with echo, in upper case.
func shout() Handler[echo, echo] {
	return func(_ context.Context, e echo) (echo, error) {
		e.Text = strings.ToUpper(e.Text)
		return e, nil
	}
}

// convertedUnary serves UnaryCall with shout, its messages converted by conv.
func convertedUnary(conv Conversion) ServiceConfig {
	return ServiceConfig{
		Desc:    &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{{Method: "UnaryCall", Handler: HandlerAs[echo, echo](shout), Convert: conv}},
	}
}

// assertCode checks the code of err, and the Reason of its errdetails.ErrorInfo.
func assertCode(t testing.TB, err error, code codes.Code, reason string) {
	t.Helper()
	s, _ := status.FromError(err)
	if s.Code() != code || errorInfo(s).GetReason() != reason {
		t.Fatalf("got %v, want %s with reason %q", err, code, reason)
	}
}

func TestConvert(t *testing.T) {
	h := newHarness(t, Config{Services: []ServiceConfig{convertedUnary(Convert(echoOf, echoMessage))}})
	resp, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{ResponseSize: 3, Payload: &testpb.Payload{Body: []byte("ada")}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetUsername() != "ADA" || len(resp.GetPayload().GetBody()) != 3 {
		t.Fatalf("got %v", resp)
	}
	// The messages are logged, not the structs.
	if body := h.line(t, "operation started")["req_body"]; !jsonEqual(body, `{"response_size": 3, "payload": {"body": "YWRh"}}`) {
		t.Fatalf("req_body %v", body)
	}
	if body := h.line(t, "operation completed")["resp_body"]; !jsonEqual(body, `{"username": "ADA", "payload": {"body": "AAAA"}}`) {
		t.Fatalf("resp_body %v", body)
	}

	// The request the functions return is validated.
	_, err = h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{ResponseSize: -1})
	assertCode(t, err, codes.InvalidArgument, "validation")
}

func TestConvertErrors(t *testing.T) {
	outOfStock := operation.Error{Code: "out_of_stock", Status: operation.StatusPreconditionFailed, Message: "out of stock"}
	failRequest := func(err error) Conversion {
		return Convert(func(*testpb.SimpleRequest) (echo, error) { return echo{}, err }, echoMessage)
	}
	failResponse := func(msg *testpb.SimpleResponse, err error) Conversion {
		return Convert(echoOf, func(echo) (*testpb.SimpleResponse, error) { return msg, err })
	}
	for _, tc := range []struct {
		name   string
		conv   Conversion
		code   codes.Code
		msg    string
		reason string
		logged string
	}{
		{"request", failRequest(errors.New("sku x-1 unknown")), codes.InvalidArgument, "invalid request", "binding",
			"invalid request: sku x-1 unknown"},
		{"request operation error", failRequest(outOfStock), codes.FailedPrecondition, "out of stock", "out_of_stock",
			"out of stock"},
		{"response", failResponse(nil, errors.New("quantity out of range")), codes.Internal, "invalid response",
			"invalid_response", "invalid response: quantity out of range"},
		{"response operation error", failResponse(nil, outOfStock), codes.FailedPrecondition, "out of stock",
			"out_of_stock", "out of stock"},
		{"nil response", failResponse(nil, nil), codes.Internal, "handler returned no response", "nil_response",
			"handler returned no response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, Config{Services: []ServiceConfig{convertedUnary(tc.conv)}})
			_, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
			assertStatus(t, err, tc.code, tc.msg, tc.reason)
			if logged := h.line(t, "operation failed")["error"]; logged != tc.logged {
				t.Fatalf("logged %v, want %q", logged, tc.logged)
			}
		})
	}
}

// outRequest, chunk and total are the business types of the streaming methods of grpc.testing.TestService.
type outRequest struct{ Sizes []int }

type chunk struct{ Size int }

type total struct{ Size int }

func outRequestOf(m *testpb.StreamingOutputCallRequest) (outRequest, error) {
	var r outRequest
	for _, p := range m.GetResponseParameters() {
		r.Sizes = append(r.Sizes, int(p.GetSize()))
	}
	return r, nil
}

func chunkMessage(c chunk) (*testpb.StreamingOutputCallResponse, error) {
	return &testpb.StreamingOutputCallResponse{Payload: &testpb.Payload{Body: make([]byte, c.Size)}}, nil
}

func chunkOf(m *testpb.StreamingInputCallRequest) (chunk, error) {
	return chunk{Size: len(m.GetPayload().GetBody())}, nil
}

func totalMessage(t total) (*testpb.StreamingInputCallResponse, error) {
	return &testpb.StreamingInputCallResponse{AggregatedPayloadSize: int32(t.Size)}, nil
}

// Streams convert every message they receive and send.
func TestConvertStreams(t *testing.T) {
	sendAll := func(r outRequest, send func(chunk) error) error {
		for _, size := range r.Sizes {
			if err := send(chunk{Size: size}); err != nil {
				return err
			}
		}
		return nil
	}
	h := newHarness(t, Config{Services: []ServiceConfig{{
		Desc: &testpb.TestService_ServiceDesc,
		Methods: []MethodConfig{
			{
				Method: "StreamingOutputCall",
				Handler: ServerStreamAs[outRequest, chunk](func() ServerStreamHandler[outRequest, chunk] {
					return func(_ context.Context, r outRequest, send func(chunk) error) error { return sendAll(r, send) }
				}),
				Convert: Convert(outRequestOf, chunkMessage),
			},
			{
				Method: "StreamingInputCall",
				Handler: ClientStreamAs[chunk, total](func() ClientStreamHandler[chunk, total] {
					return func(_ context.Context, chunks iter.Seq2[chunk, error]) (total, error) {
						var sum total
						for c, err := range chunks {
							if err != nil {
								return total{}, err
							}
							sum.Size += c.Size
						}
						return sum, nil
					}
				}),
				Convert: Convert(chunkOf, totalMessage),
			},
			{
				Method: "FullDuplexCall",
				Handler: BidiStreamAs[outRequest, chunk](func() BidiStreamHandler[outRequest, chunk] {
					return func(_ context.Context, reqs iter.Seq2[outRequest, error], send func(chunk) error) error {
						for r, err := range reqs {
							if err != nil {
								return err
							}
							if err := sendAll(r, send); err != nil {
								return err
							}
						}
						return nil
					}
				}),
				Convert: Convert(outRequestOf, chunkMessage),
			},
		},
	}}})
	ctx := testCtx(t)
	params := []*testpb.ResponseParameters{{Size: 1}, {Size: 2}}

	out, err := h.client.StreamingOutputCall(ctx, &testpb.StreamingOutputCallRequest{ResponseParameters: params})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []int{1, 2} {
		resp, err := out.Recv()
		if err != nil || len(resp.GetPayload().GetBody()) != want {
			t.Fatalf("server stream: got %v, %v", resp, err)
		}
	}
	if _, err := out.Recv(); err != io.EOF {
		t.Fatalf("server stream: got %v, want the end", err)
	}

	in, err := h.client.StreamingInputCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"ab", "cde"} {
		if err := in.Send(&testpb.StreamingInputCallRequest{Payload: &testpb.Payload{Body: []byte(body)}}); err != nil {
			t.Fatal(err)
		}
	}
	if resp, err := in.CloseAndRecv(); err != nil || resp.GetAggregatedPayloadSize() != 5 {
		t.Fatalf("client stream: got %v, %v", resp, err)
	}

	bidi, err := h.client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := bidi.Send(&testpb.StreamingOutputCallRequest{ResponseParameters: params[1:]}); err != nil {
			t.Fatal(err)
		}
		if resp, err := bidi.Recv(); err != nil || len(resp.GetPayload().GetBody()) != 2 {
			t.Fatalf("bidi stream: got %v, %v", resp, err)
		}
	}
	if err := bidi.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := bidi.Recv(); err != io.EOF {
		t.Fatalf("bidi stream: got %v, want the end", err)
	}
}

// New fails on a Conversion that does not fit the handler's types or the method's messages.
func TestConvertConfigErrors(t *testing.T) {
	unary := func(handler HandlerFactory, conv Conversion) Config {
		return Config{Services: []ServiceConfig{{
			Desc:    &testpb.TestService_ServiceDesc,
			Methods: []MethodConfig{{Method: "UnaryCall", Handler: handler, Convert: conv}},
		}}}
	}
	for _, tc := range []struct {
		name string
		cfg  Config
		want string
	}{
		{
			"nil function",
			unary(HandlerAs[echo, echo](shout), Convert[echo, echo, testpb.SimpleRequest, testpb.SimpleResponse](nil, echoMessage)),
			`grpc: service "grpc.testing.TestService" method "UnaryCall": nil function in Convert`,
		},
		{
			"request type",
			unary(HandlerAs[other, echo](func() Handler[other, echo] { return nil }), Convert(echoOf, echoMessage)),
			`grpc: service "grpc.testing.TestService" method "UnaryCall": the request function of Convert returns grpc.echo, but the handler takes grpc.other`,
		},
		{
			"response type",
			unary(HandlerAs[echo, other](func() Handler[echo, other] { return nil }), Convert(echoOf, echoMessage)),
			`grpc: service "grpc.testing.TestService" method "UnaryCall": the response function of Convert takes grpc.echo, but the handler returns grpc.other`,
		},
		{
			"request message",
			unary(HandlerAs[echo, echo](shout), Convert(func(*testpb.Empty) (echo, error) { return echo{}, nil }, echoMessage)),
			`grpc: service "grpc.testing.TestService" method "UnaryCall": the request function of Convert takes grpc.testing.Empty, but the method's request is grpc.testing.SimpleRequest`,
		},
		{
			"response message",
			unary(HandlerAs[echo, echo](shout), Convert(echoOf, func(echo) (*testpb.Empty, error) { return nil, nil })),
			`grpc: service "grpc.testing.TestService" method "UnaryCall": the response function of Convert returns grpc.testing.Empty, but the method's response is grpc.testing.SimpleResponse`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := newServerErr(t, tc.cfg); err == nil || err.Error() != tc.want {
				t.Fatalf("got %v\nwant %s", err, tc.want)
			}
		})
	}
}

// A converted request and response cost the server nothing besides the functions: no reflection, and no
// allocation of its own when nothing validates the request.
func TestConvertDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	reply := &testpb.SimpleResponse{}
	conv := Convert(func(m *testpb.SimpleRequest) (echo, error) { return echo{Size: int(m.GetResponseSize())}, nil },
		func(echo) (*testpb.SimpleResponse, error) { return reply, nil })
	b, err := convertedBindings[echo, echo](conv, methodMessages{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	msg, ctx := &testpb.SimpleRequest{ResponseSize: 3}, context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		req, err := bindRequest(buildEnv{}, b.req, ctx, msg)
		if err != nil || req.Size != 3 {
			t.Fatalf("got %v, %v", req, err)
		}
		if m, err := b.resp.toProto(req); err != nil || m != reply {
			t.Fatalf("got %v, %v", m, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocated %v times", allocs)
	}
}

// A request of another message than the request function's, which a middleware put in State, fails.
func TestConvertOtherMessage(t *testing.T) {
	b, err := convertedBindings[echo, echo](Convert(echoOf, echoMessage), methodMessages{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.req.toGo(context.Background(), &testpb.Empty{})
	if want := "grpc: the request is *grpc_testing.Empty, not *grpc_testing.SimpleRequest"; err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}
