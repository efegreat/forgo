package grpc

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// argKey is the type of the Args keys the gRPC server reads. Its own type keeps them apart from the
// keys of every other package.
type argKey uint8

const (
	// ArgKeyRecorderDisabled turns the Recorder off for the methods whose Args map it to true, such as
	// health checks.
	ArgKeyRecorderDisabled argKey = iota
)

// OperationTypeGRPCHandler is the type of the gRPC server's operations, logged as op_type.
const OperationTypeGRPCHandler = "grpc_handler"

// ReqIDMetadata carries the operation ID. The ID middleware keeps a well-formed one from the request
// metadata, and the server returns the operation's ID under it in the trailer of every call.
const ReqIDMetadata = "x-request-id"

// unimplementedName is the operation name of the calls no configured method matches.
const unimplementedName = "unimplemented"

var (
	errUnimplemented = operation.Error{
		Code:    "unimplemented",
		Status:  operation.StatusUnimplemented,
		Message: "unknown method %s",
	}
	errNilResponse = operation.Error{
		Code:     "nil_response",
		Status:   operation.StatusInternal,
		Message:  "handler returned no response",
		Severity: operation.SeverityCritical,
	}
	errCanceled = operation.Error{
		Code:    "canceled",
		Status:  operation.StatusCancelled,
		Message: "call canceled",
	}
	errDeadlineExceeded = operation.Error{
		Code:    "deadline_exceeded",
		Status:  operation.StatusTimeout,
		Message: "deadline exceeded",
	}
	// The errors of failed receives and sends describe a status grpc-go already sent, so their
	// Message is only logged.
	errMessageTooLarge = operation.Error{
		Code:    "message_too_large",
		Status:  operation.StatusTooLarge,
		Message: "request message too large",
	}
	errInvalidMessage = operation.Error{
		Code:    "invalid_message",
		Message: "invalid request message",
	}
	errSendFailed = operation.Error{
		Code:     "send_failed",
		Message:  "sending a response message failed",
		Severity: operation.SeverityModerate,
	}
)

// recorderDisabled reports whether the Args of the operation turn the Recorder off.
func recorderDisabled(ctx *operation.Context) bool {
	return ctx.Args.GetTyped[bool](ArgKeyRecorderDisabled)
}

// State is the transport state of a gRPC operation, in operation.Context.State. Middlewares read it with
// ctx.State.(*grpc.State), and handlers through operation.FromCtx.
type State struct {
	// FullMethod is the method called, such as "/user.v1.UserService/GetUser".
	FullMethod string
	// Request is the request message of a unary or server-streaming method, received before the chain
	// runs, so that middlewares and the Recorder can read it. It is nil for client and bidi streams,
	// whose handlers receive their messages, for an unknown method, and when receiving failed.
	Request proto.Message
	// Response is the response of a unary or client-streaming method once the handler sends it, nil
	// before and for other methods.
	Response proto.Message

	stream      grpc.ServerStream
	terminating <-chan struct{}
	// failure is the first receive or send that failed. grpc-go answers such a failure with a status of
	// its own as soon as it happens, so nothing answers the call after it.
	failure atomic.Pointer[streamFailure]
	// answer is the status ErrorResponder answered the call with, nil until then.
	answer *status.Status
}

// Metadata returns a copy of the metadata the client sent.
func (s *State) Metadata() metadata.MD {
	md, _ := metadata.FromIncomingContext(s.stream.Context())
	return md
}

// Peer returns the client of the call, and false when grpc-go does not know it.
func (s *State) Peer() (*peer.Peer, bool) {
	return peer.FromContext(s.stream.Context())
}

// Terminating returns a channel that is closed when the server starts terminating. Long-lived streams
// return when it closes, since Terminate waits for every call to end.
func (s *State) Terminating() <-chan struct{} {
	return s.terminating
}

// Code returns the status code of the call: the one grpc-go sent for a failed receive or send, else the
// one ErrorResponder answered with, else codes.OK.
func (s *State) Code() codes.Code {
	if f := s.failure.Load(); f != nil {
		return f.status.Code()
	}
	if s.answer != nil {
		return s.answer.Code()
	}
	return codes.OK
}

// answered reports whether the call has its status: grpc-go sent one for a failed receive or send, or
// ErrorResponder chose one.
func (s *State) answered() bool {
	return s.answer != nil || s.failure.Load() != nil
}

// direction tells a receive from a send.
type direction bool

const (
	receive direction = false
	send    direction = true
)

// streamFailure is a receive or send that failed, which grpc-go answered with status.
type streamFailure struct {
	status *status.Status
	cause  error           // the error RecvMsg or SendMsg returned
	err    operation.Error // what the operation fails with
}

// fail records a failed receive or send of the call, and returns the error the operation fails with. Only
// the first failure is recorded, since grpc-go sends only the first status. It is safe for concurrent use,
// since bidi handlers may receive and send from two goroutines.
func (s *State) fail(dir direction, err error) error {
	f := &streamFailure{status: status.Convert(err), cause: err}
	f.err = failureError(dir, f.status.Code()).Wrap(err)
	if !s.failure.CompareAndSwap(nil, f) {
		f = s.failure.Load()
	}
	return f.err
}

// result returns the error a stream handler's operation fails with, given what the handler returned.
// After a failed receive or send, the handler's nil, or its error that holds that failure, becomes the
// failure's error, which has the code grpc-go answered.
func (s *State) result(err error) error {
	f := s.failure.Load()
	if f == nil {
		return err
	}
	switch {
	case err == nil:
		return f.err
	case errors.Is(err, f.cause):
		return f.err.Wrap(err)
	default:
		return err
	}
}

// failureError returns the error of a receive or send that failed with code.
func failureError(dir direction, code codes.Code) operation.Error {
	switch {
	case code == codes.Canceled:
		return errCanceled
	case code == codes.DeadlineExceeded:
		return errDeadlineExceeded
	case dir == receive && code == codes.ResourceExhausted:
		return errMessageTooLarge
	case dir == receive:
		e := errInvalidMessage
		e.Status = operationStatus(code)
		return e
	default:
		e := errSendFailed
		e.Status = operationStatus(code)
		return e
	}
}

// opStream is the stream stream handlers get. Its Context is the operation's, and it records failed
// receives and sends, validates received messages, and keeps the response of a client stream for the
// log.
type opStream struct {
	grpc.ServerStream
	op *operation.Context
	st *State
	// validate validates received messages. It is nil when the request was received before the chain.
	validate func(proto.Message) error
	// keepResponse keeps the sent message in State.Response, for a client stream, which sends one.
	keepResponse bool
}

// Context returns the operation, which holds the call's context.
func (s *opStream) Context() context.Context {
	return s.op
}

// RecvMsg receives a message. A failure other than the end of the stream is recorded, and a message the
// validator rejects is returned with its validation error.
func (s *opStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		if err != io.EOF {
			s.st.fail(receive, err)
		}
		return err
	}
	if s.validate != nil {
		if msg, ok := m.(proto.Message); ok {
			return validateMessage(s.validate, msg)
		}
	}
	return nil
}

// SendMsg sends a message. A failure is recorded.
func (s *opStream) SendMsg(m any) error {
	if s.keepResponse {
		s.st.Response, _ = m.(proto.Message)
	}
	if err := s.ServerStream.SendMsg(m); err != nil {
		s.st.fail(send, err)
		return err
	}
	return nil
}
