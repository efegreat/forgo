package grpc

import (
	"errors"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	protoencoding "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// errUndecodable is a request message that cannot be decoded, such as one whose string field is not UTF-8.
// Unlike the other receive failures, grpc-go has not answered it, and the server answers it like a binding
// error. Its message holds the codec's reason, as grpc-go's and connect-go's do: protobuf's names at most a
// field, never a value, and protobuf varies its "proto:" prefix so that nothing matches on it. It has no cause,
// which would only repeat the reason in the log.
var errUndecodable = operation.Error{
	Code:    "invalid_message",
	Status:  operation.StatusInvalidArgument,
	Message: "invalid request message: %s",
}

// undecodable reports whether err holds errUndecodable. The receive failures grpc-go answered are status
// errors, never an operation.Error of its code.
func undecodable(err error) bool {
	return errors.Is(err, errUndecodable)
}

// codec is the codec of every call the server serves: the codec registered for protobuf, except that it never
// fails to decode a decodeTarget. grpc-go answers a codec's error with Internal at once, as if the server had
// failed, so the server receives into a decodeTarget and answers the decoding error itself.
type codec struct {
	encoding.CodecV2
}

func newCodec() codec {
	return codec{CodecV2: encoding.GetCodecV2(protoencoding.Name)}
}

func (c codec) Unmarshal(data mem.BufferSlice, v any) error {
	if t, ok := v.(*decodeTarget); ok {
		t.err = c.CodecV2.Unmarshal(data, t.msg)
		return nil
	}
	return c.CodecV2.Unmarshal(data, v)
}

// decodeTarget is a message the server receives, and the error decoding it failed with. Stats handlers get it
// as the Payload of their stats.InPayload, and its ProtoReflect is the message's, so that it reads like the
// message.
type decodeTarget struct {
	msg proto.Message
	err error
}

func (t *decodeTarget) ProtoReflect() protoreflect.Message {
	return t.msg.ProtoReflect()
}

// decodeStream is grpc-go's stream of a call, below every wrapper. It receives a message into a decodeTarget,
// so that a message that cannot be decoded fails with errUndecodable rather than with grpc-go's status.
type decodeStream struct {
	grpc.ServerStream
	// target is reused, since grpc-go receives on a stream from one goroutine at a time. It keeps the last
	// message, so that a stats handler that holds on to its InPayload, which grpc-go allows it to change
	// afterwards, still reads a message.
	target decodeTarget
}

// RecvMsg receives a message into m. A message that cannot be decoded is errUndecodable, which grpc-go did not
// answer.
func (s *decodeStream) RecvMsg(m any) error {
	msg, ok := m.(proto.Message)
	if !ok {
		return s.ServerStream.RecvMsg(m)
	}
	s.target = decodeTarget{msg: msg}
	err := s.ServerStream.RecvMsg(&s.target)
	if err == nil && s.target.err != nil {
		err = errUndecodable.Format(s.target.err)
	}
	return err
}
