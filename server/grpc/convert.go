package grpc

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// errInvalidRequest wraps the error of a Convert request function that holds no operation.Error. Its cause,
// which may quote the request, is only logged.
var errInvalidRequest = operation.Error{
	Code:    "binding",
	Status:  operation.StatusInvalidArgument,
	Message: "invalid request",
}

// Conversion converts the messages of a method with functions of the application's own, in place of the
// conversion New plans from the struct tags. Convert creates one. Its zero value plans the conversion.
//
// A call runs the functions alone, without reflection. They take over what the planned conversion checks: New
// no longer fails on a request field without a struct field, or a response field without a message field,
// numbers are converted as the functions convert them, and fields tagged header are not filled from the
// call's metadata. Validation still runs on the request they return, and the messages are logged as before.
type Conversion struct {
	// req and resp are the handler's types the functions convert, and in and out the messages, which New
	// checks against the handler's and the method's.
	req, resp  reflect.Type
	in, out    protoreflect.FullName
	newMessage func() proto.Message
	decode     any // func(proto.Message) (Req, error)
	encode     any // func(Resp) (proto.Message, error)
	// err is a mistake Convert found, which New reports.
	err error
}

// Convert returns the Conversion of a method: request converts its request message to the handler's request,
// and response converts the handler's response to its response message. Streaming methods convert every
// message they receive and send with them. New fails when the functions do not take and return the handler's
// types and the method's messages. Keep the functions of a service in a file of their own beside the server's
// configuration, which then only names them.
//
// An error of either function that holds an operation.Error is answered as it is. Any other error of request
// is answered InvalidArgument with errCode "binding" and the message "invalid request", and any other error of
// response Internal with errCode "invalid_response", like the errors of the conversions New plans. Their text
// is logged, never sent. A nil message from response is answered Internal with errCode "nil_response".
func Convert[Req, Resp, In, Out any, PIn interface {
	*In
	proto.Message
}, POut interface {
	*Out
	proto.Message
}](request func(PIn) (Req, error), response func(Resp) (POut, error)) Conversion {
	c := Conversion{
		req:  reflect.TypeFor[Req](),
		resp: reflect.TypeFor[Resp](),
		in:   PIn(new(In)).ProtoReflect().Descriptor().FullName(),
		out:  POut(new(Out)).ProtoReflect().Descriptor().FullName(),
	}
	if request == nil || response == nil {
		c.err = errors.New("nil function in Convert")
		return c
	}
	c.newMessage = func() proto.Message { return PIn(new(In)) }
	c.decode = func(m proto.Message) (Req, error) {
		msg, ok := m.(PIn)
		if !ok {
			var zero Req
			return zero, fmt.Errorf("grpc: the request is %T, not %T", m, msg)
		}
		req, err := request(msg)
		if err != nil {
			if _, ok := errors.AsType[operation.Error](err); !ok {
				err = errInvalidRequest.Wrap(err)
			}
		}
		return req, err
	}
	c.encode = func(v Resp) (proto.Message, error) {
		msg, err := response(v)
		switch {
		case err != nil:
			if _, ok := errors.AsType[operation.Error](err); !ok {
				err = errInvalidResponse.Wrap(err)
			}
			return nil, err
		case msg == nil:
			return nil, errNilResponse
		}
		return msg, nil
	}
	return c
}

// planned reports whether c is the zero Conversion, which plans the conversion.
func (c Conversion) planned() bool {
	return c.req == nil
}

// convertedBindings returns the bindings of a method whose messages c converts, for the handler's types Req
// and Resp and the method's messages mm, which are unknown when the service's descriptor is not registered.
// validate is the validator of the requests, nil when nothing validates them.
func convertedBindings[Req, Resp any](c Conversion, mm methodMessages, validate func(any) error) (bindings[Req, Resp], error) {
	if c.err != nil {
		return bindings[Req, Resp]{}, c.err
	}
	decode, ok := c.decode.(func(proto.Message) (Req, error))
	if !ok {
		return bindings[Req, Resp]{}, fmt.Errorf("the request function of Convert returns %s, but the handler takes %s",
			c.req, reflect.TypeFor[Req]())
	}
	encode, ok := c.encode.(func(Resp) (proto.Message, error))
	if !ok {
		return bindings[Req, Resp]{}, fmt.Errorf("the response function of Convert takes %s, but the handler returns %s",
			c.resp, reflect.TypeFor[Resp]())
	}
	if mm.input != nil && mm.input.FullName() != c.in {
		return bindings[Req, Resp]{}, fmt.Errorf("the request function of Convert takes %s, but the method's request is %s",
			c.in, mm.input.FullName())
	}
	if mm.output != nil && mm.output.FullName() != c.out {
		return bindings[Req, Resp]{}, fmt.Errorf("the response function of Convert returns %s, but the method's response is %s",
			c.out, mm.output.FullName())
	}

	b := bindings[Req, Resp]{
		req:  binding[Req]{name: c.req.String(), newMessage: c.newMessage, validate: validate},
		resp: binding[Resp]{name: c.resp.String(), toProto: encode},
	}
	// The validator gets a pointer to the request, which only a request that is validated pays for.
	switch {
	case validate == nil:
		b.req.decode = func(_ context.Context, m proto.Message) (Req, any, error) {
			req, err := decode(m)
			return req, nil, err
		}
	case c.req.Kind() == reflect.Pointer:
		b.req.decode = func(_ context.Context, m proto.Message) (Req, any, error) {
			req, err := decode(m)
			return req, req, err
		}
	default:
		b.req.decode = func(_ context.Context, m proto.Message) (Req, any, error) {
			req, err := decode(m)
			return req, &req, err
		}
	}
	return b, nil
}
