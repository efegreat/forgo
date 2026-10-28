package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"reflect"
	"slices"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/redact"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// methodKind is the streaming kind of a method.
type methodKind uint8

const (
	kindUnary methodKind = iota
	kindServerStream
	kindClientStream
	kindBidiStream
)

func (k methodKind) String() string {
	switch k {
	case kindUnary:
		return "unary"
	case kindServerStream:
		return "server-streaming"
	case kindClientStream:
		return "client-streaming"
	default:
		return "bidi-streaming"
	}
}

// receivesFirst reports whether the server receives the one request of the method before the chain runs.
func (k methodKind) receivesFirst() bool {
	return k == kindUnary || k == kindServerStream
}

// respondsOnce reports whether the method has one response, which the Responder sends after the chain.
func (k methodKind) respondsOnce() bool {
	return k == kindUnary || k == kindClientStream
}

func (k methodKind) clientStreams() bool {
	return k == kindClientStream || k == kindBidiStream
}

func (k methodKind) serverStreams() bool {
	return k == kindServerStream || k == kindBidiStream
}

// streamKind returns the kind of a stream from its flags.
func streamKind(clientStreams, serverStreams bool) methodKind {
	switch {
	case clientStreams && serverStreams:
		return kindBidiStream
	case clientStreams:
		return kindClientStream
	case serverStreams:
		return kindServerStream
	default:
		return kindUnary
	}
}

// Handler serves a unary method: it gets the request and returns the response or an error. ctx is the call's
// *operation.Context.
//
// Req and Resp are usually the handler's own structs, which the server converts from and to the method's
// proto messages, so that business code needs no generated type: the very method that serves an HTTP route
// with http.HandlerFor serves a gRPC method with HandlerFor. They can also be the proto messages themselves,
// such as *userpb.GetUserRequest, which are passed as they are.
type Handler[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)

// ServerStreamHandler serves a server-streaming method: it gets the request, and sends responses with send
// until it returns. send fails once the client is gone, and the handler then returns its error.
type ServerStreamHandler[Req, Resp any] func(ctx context.Context, req Req, send func(Resp) error) error

// ClientStreamHandler serves a client-streaming method: it ranges over the requests the client sends, and
// returns the response once they end. A request that cannot be received, decoded, converted or validated ends
// the range with its error, which the handler returns.
type ClientStreamHandler[Req, Resp any] func(ctx context.Context, reqs iter.Seq2[Req, error]) (Resp, error)

// BidiStreamHandler serves a bidi-streaming method: it ranges over the requests and sends responses, which it
// may do from two goroutines. It returns once it is done, and the call ends with it.
type BidiStreamHandler[Req, Resp any] func(ctx context.Context, reqs iter.Seq2[Req, error], send func(Resp) error) error

// HandlerFactory builds the handler of a method when the server is created. Create one with HandlerFor,
// HandlerAs, ServerStreamFor, ClientStreamFor, BidiStreamFor or their As variants. New checks its kind
// against the method's, and its request and response types against the method's messages.
type HandlerFactory struct {
	kind methodKind
	// build checks the types of the handler against the messages of the method and resolves the handler.
	build func(forgo.Resolver, buildEnv, methodMessages) (builtHandler, error)
}

// buildEnv is what the handlers of a server depend on besides the bootstrap.
type buildEnv struct {
	termSig  *forgo.TermSig // nil when unset
	services []string       // the services the server serves
	// validate is Config.Validator.
	validate func(any) error
	// validates reports whether validate, the default Validator, checks anything on a request type, and
	// panics on a validate tag it does not know. It is nil with a Validator of the configuration's own, which
	// runs on every request.
	validates func(reflect.Type) bool
	// sensitive masks the input that binding errors quote.
	sensitive sensitiveFields
	// logs are the messageLogs planned so far, by message, which the methods share.
	logs map[protoreflect.FullName]*messageLog
}

// maskBindingError returns err without the value it quotes when that value is sensitive: a field whose path
// holds a SensitiveFields name, or a metadata key that is masked in logs.
func (e buildEnv) maskBindingError(err error) error {
	var fe *fieldError
	if !errors.As(err, &fe) {
		return err
	}
	names := e.sensitive.fields
	if fe.source == sourceMetadata {
		names = e.sensitive.metadata
	}
	if !slices.ContainsFunc(fe.path, names.Has) {
		return err
	}
	masked := *fe
	masked.cause = redact.MaskValueError(fe.cause)
	return &masked
}

// methodMessages are the messages of a method, from the proto registry. They are nil when the service's
// descriptor is not registered.
type methodMessages struct {
	input, output protoreflect.MessageDescriptor
}

// builtHandler is the terminal handler of a method, and the request messages it receives.
type builtHandler struct {
	handler    operation.Handler
	newRequest func() proto.Message
}

// bindings converts the messages of a method and the types of its handler.
type bindings[Req, Resp any] struct {
	req  binding[Req]
	resp binding[Resp]
}

// HandlerFor serves a unary method with a method of a service the bootstrap provides, such as
// HandlerFor((*user.Service).Get). The receiver S is resolved once, in New, and can be an interface, as in
// HandlerFor(user.Reader.Get).
func HandlerFor[S, Req, Resp any](fn func(S, context.Context, Req) (Resp, error)) HandlerFactory {
	return unaryFactory(func(r forgo.Resolver, _ buildEnv) (Handler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, req Req) (Resp, error) {
			return fn(service, ctx, req)
		}, nil
	})
}

// HandlerAs serves a unary method with the Handler that constructor returns. The constructor's parameters
// are resolved from the bootstrap, once, in New.
func HandlerAs[Req, Resp any](constructor any) HandlerFactory {
	return unaryFactory(func(r forgo.Resolver, _ buildEnv) (Handler[Req, Resp], error) {
		return r.Call[Handler[Req, Resp]](constructor)
	})
}

// ServerStreamFor serves a server-streaming method with a method of a service the bootstrap provides, such
// as ServerStreamFor((*user.Service).Watch).
func ServerStreamFor[S, Req, Resp any](fn func(S, context.Context, Req, func(Resp) error) error) HandlerFactory {
	return serverStreamFactory(func(r forgo.Resolver, _ buildEnv) (ServerStreamHandler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, req Req, send func(Resp) error) error {
			return fn(service, ctx, req, send)
		}, nil
	})
}

// ServerStreamAs serves a server-streaming method with the ServerStreamHandler that constructor returns.
func ServerStreamAs[Req, Resp any](constructor any) HandlerFactory {
	return serverStreamFactory(func(r forgo.Resolver, _ buildEnv) (ServerStreamHandler[Req, Resp], error) {
		return r.Call[ServerStreamHandler[Req, Resp]](constructor)
	})
}

// ClientStreamFor serves a client-streaming method with a method of a service the bootstrap provides, such
// as ClientStreamFor((*user.Service).Import).
func ClientStreamFor[S, Req, Resp any](fn func(S, context.Context, iter.Seq2[Req, error]) (Resp, error)) HandlerFactory {
	return clientStreamFactory(func(r forgo.Resolver, _ buildEnv) (ClientStreamHandler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, reqs iter.Seq2[Req, error]) (Resp, error) {
			return fn(service, ctx, reqs)
		}, nil
	})
}

// ClientStreamAs serves a client-streaming method with the ClientStreamHandler that constructor returns.
func ClientStreamAs[Req, Resp any](constructor any) HandlerFactory {
	return clientStreamFactory(func(r forgo.Resolver, _ buildEnv) (ClientStreamHandler[Req, Resp], error) {
		return r.Call[ClientStreamHandler[Req, Resp]](constructor)
	})
}

// BidiStreamFor serves a bidi-streaming method with a method of a service the bootstrap provides, such as
// BidiStreamFor((*chat.Service).Talk).
func BidiStreamFor[S, Req, Resp any](fn func(S, context.Context, iter.Seq2[Req, error], func(Resp) error) error) HandlerFactory {
	return bidiStreamFactory(func(r forgo.Resolver, _ buildEnv) (BidiStreamHandler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, reqs iter.Seq2[Req, error], send func(Resp) error) error {
			return fn(service, ctx, reqs, send)
		}, nil
	})
}

// BidiStreamAs serves a bidi-streaming method with the BidiStreamHandler that constructor returns.
func BidiStreamAs[Req, Resp any](constructor any) HandlerFactory {
	return bidiStreamFactory(func(r forgo.Resolver, _ buildEnv) (BidiStreamHandler[Req, Resp], error) {
		return r.Call[BidiStreamHandler[Req, Resp]](constructor)
	})
}

// newFactory returns a HandlerFactory of kind: resolve builds the handler fn, and terminal the terminal
// handler that serves it. The bindings of its messages are built first, so a type that does not fit fails
// before anything is resolved.
func newFactory[Req, Resp, F any](
	kind methodKind,
	resolve func(forgo.Resolver, buildEnv) (F, error),
	terminal func(F, bindings[Req, Resp], buildEnv) operation.Handler,
) HandlerFactory {
	return HandlerFactory{
		kind: kind,
		build: func(r forgo.Resolver, env buildEnv, mm methodMessages) (builtHandler, error) {
			req, err := newBinding[Req](mm.input, true)
			if err != nil {
				return builtHandler{}, fmt.Errorf("request type: %w", err)
			}
			if env.validates == nil || env.validates(reflect.TypeFor[Req]()) {
				req.validate = env.validate
			}
			resp, err := newBinding[Resp](mm.output, false)
			if err != nil {
				return builtHandler{}, fmt.Errorf("response type: %w", err)
			}
			fn, err := resolve(r, env)
			if err != nil {
				return builtHandler{}, err
			}
			return builtHandler{
				handler:    terminal(fn, bindings[Req, Resp]{req: req, resp: resp}, env),
				newRequest: req.newMessage,
			}, nil
		},
	}
}

func unaryFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (Handler[Req, Resp], error)) HandlerFactory {
	return newFactory(kindUnary, resolve, unaryTerminal[Req, Resp])
}

func serverStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (ServerStreamHandler[Req, Resp], error)) HandlerFactory {
	return newFactory(kindServerStream, resolve, serverStreamTerminal[Req, Resp])
}

func clientStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (ClientStreamHandler[Req, Resp], error)) HandlerFactory {
	return newFactory(kindClientStream, resolve, clientStreamTerminal[Req, Resp])
}

func bidiStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (BidiStreamHandler[Req, Resp], error)) HandlerFactory {
	return newFactory(kindBidiStream, resolve, bidiStreamTerminal[Req, Resp])
}

// grpcBidiStreamFactory serves a bidi-streaming method with a handler of grpc-go's generated shape, such as
// that of the reflection service.
func grpcBidiStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (func(grpc.BidiStreamingServer[Req, Resp]) error, error)) HandlerFactory {
	return newFactory(kindBidiStream, resolve, func(fn func(grpc.BidiStreamingServer[Req, Resp]) error, _ bindings[*Req, *Resp], _ buildEnv) operation.Handler {
		return func(op *operation.Context) error {
			st := op.State.(*State)
			return st.result(fn(&grpc.GenericServerStream[Req, Resp]{ServerStream: newOpStream(op, st)}))
		}
	})
}

// unaryTerminal builds the terminal handler of a unary method: it converts and validates the request the
// server received, calls fn and leaves its response in State.Response, for the Responder to send.
func unaryTerminal[Req, Resp any](fn Handler[Req, Resp], b bindings[Req, Resp], env buildEnv) operation.Handler {
	return func(op *operation.Context) error {
		st := op.State.(*State)
		if f := st.failure.Load(); f != nil {
			return f.err
		}
		if st.undecodable != nil {
			return st.undecodable
		}
		req, err := bindRequest(env, b.req, op, st.Request)
		if err != nil {
			return err
		}
		resp, err := fn(op, req)
		if err != nil {
			return err
		}
		msg, err := b.resp.toProto(resp)
		if err != nil {
			return err
		}
		st.Response = msg
		return nil
	}
}

// serverStreamTerminal builds the terminal handler of a server-streaming method: it receives the request
// through the stream, as grpc-go's generated handlers do, so that the stream's wrappers see it, converts and
// validates it, and calls fn.
func serverStreamTerminal[Req, Resp any](fn ServerStreamHandler[Req, Resp], b bindings[Req, Resp], env buildEnv) operation.Handler {
	return func(op *operation.Context) error {
		st := op.State.(*State)
		stream := newOpStream(op, st)
		msg := st.Request
		if msg == nil {
			msg = b.req.newMessage()
		}
		if err := stream.RecvMsg(msg); err != nil {
			return st.result(err)
		}
		req, err := bindRequest(env, b.req, op, msg)
		if err != nil {
			return err
		}
		return st.result(fn(op, req, sender(b.resp, stream)))
	}
}

// clientStreamTerminal builds the terminal handler of a client-streaming method: it calls fn and leaves its
// response in State.Response, for the Responder to send.
func clientStreamTerminal[Req, Resp any](fn ClientStreamHandler[Req, Resp], b bindings[Req, Resp], env buildEnv) operation.Handler {
	return func(op *operation.Context) error {
		st := op.State.(*State)
		resp, err := fn(op, receiver(env, b.req, op, newOpStream(op, st)))
		if err == nil && st.failure.Load() == nil {
			var msg proto.Message
			if msg, err = b.resp.toProto(resp); err == nil {
				st.Response = msg
			}
		}
		return st.result(err)
	}
}

// bidiStreamTerminal builds the terminal handler of a bidi-streaming method.
func bidiStreamTerminal[Req, Resp any](fn BidiStreamHandler[Req, Resp], b bindings[Req, Resp], env buildEnv) operation.Handler {
	return func(op *operation.Context) error {
		st := op.State.(*State)
		stream := newOpStream(op, st)
		return st.result(fn(op, receiver(env, b.req, op, stream), sender(b.resp, stream)))
	}
}

// bindRequest converts msg to the handler's request and validates it. Values that cannot be converted are
// fieldErrors, masked when they are sensitive.
func bindRequest[Req any](env buildEnv, b binding[Req], ctx context.Context, msg proto.Message) (Req, error) {
	req, target, err := b.decode(ctx, msg)
	if err != nil {
		return req, env.maskBindingError(err)
	}
	if b.validate == nil {
		return req, nil
	}
	if err := b.validate(target); err != nil {
		return req, validationError{message: b.name, err: err}
	}
	return req, nil
}

// receiver returns the requests of a client or bidi stream: each message received, converted and validated.
// The first failure ends them.
func receiver[Req any](env buildEnv, b binding[Req], ctx context.Context, stream *opStream) iter.Seq2[Req, error] {
	return func(yield func(Req, error) bool) {
		for {
			msg := b.newMessage()
			if err := stream.RecvMsg(msg); err != nil {
				if err != io.EOF {
					var zero Req
					yield(zero, err)
				}
				return
			}
			req, err := bindRequest(env, b, ctx, msg)
			if !yield(req, err) || err != nil {
				return
			}
		}
	}
}

// sender returns the function that converts a response and sends it on stream.
func sender[Resp any](b binding[Resp], stream *opStream) func(Resp) error {
	return func(resp Resp) error {
		msg, err := b.toProto(resp)
		if err != nil {
			return err
		}
		return stream.SendMsg(msg)
	}
}
