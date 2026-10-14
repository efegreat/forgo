package grpc

import (
	"context"
	"reflect"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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

// Handler serves a unary method: it receives the request, validated, and returns the response or an
// error. ctx is the call's *operation.Context.
type Handler[Req, Resp any] func(ctx context.Context, req *Req) (*Resp, error)

// ServerStreamHandler serves a server-streaming method. The stream's Context is the call's
// *operation.Context.
type ServerStreamHandler[Req, Resp any] func(req *Req, stream grpc.ServerStreamingServer[Resp]) error

// ClientStreamHandler serves a client-streaming method. The stream's Context is the call's
// *operation.Context.
type ClientStreamHandler[Req, Resp any] func(stream grpc.ClientStreamingServer[Req, Resp]) error

// BidiStreamHandler serves a bidi-streaming method. The stream's Context is the call's *operation.Context.
type BidiStreamHandler[Req, Resp any] func(stream grpc.BidiStreamingServer[Req, Resp]) error

// HandlerFactory builds the handler of a method when the server is created. Create one with HandlerFor,
// HandlerAs, ServerStreamFor, ClientStreamFor, BidiStreamFor or their As variants. New checks its kind and
// message types against the service descriptor.
type HandlerFactory struct {
	// build resolves the handler.
	build      func(forgo.Resolver, buildEnv) (handlerWrapper, error)
	kind       methodKind
	req, resp  reflect.Type // message types, not pointers
	newRequest func() proto.Message
}

// buildEnv is what the handlers of a server may depend on besides the bootstrap.
type buildEnv struct {
	termSig  *forgo.TermSig // nil when unset
	services []string       // the services the server serves
}

// handlerWrapper builds the terminal handler of a method with the server's validator.
type handlerWrapper func(validate func(proto.Message) error) operation.Handler

// HandlerFor serves a unary method with a method of a service the bootstrap provides, such as
// HandlerFor((*user.Service).Get). The receiver S is resolved once, in New, and can be an interface, as in
// HandlerFor(userpb.UserServiceServer.Get).
func HandlerFor[S, Req, Resp any](fn func(S, context.Context, *Req) (*Resp, error)) HandlerFactory {
	return unaryFactory(func(r forgo.Resolver, _ buildEnv) (Handler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, req *Req) (*Resp, error) {
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
func ServerStreamFor[S, Req, Resp any](fn func(S, *Req, grpc.ServerStreamingServer[Resp]) error) HandlerFactory {
	return serverStreamFactory(func(r forgo.Resolver, _ buildEnv) (ServerStreamHandler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(req *Req, stream grpc.ServerStreamingServer[Resp]) error {
			return fn(service, req, stream)
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
func ClientStreamFor[S, Req, Resp any](fn func(S, grpc.ClientStreamingServer[Req, Resp]) error) HandlerFactory {
	return clientStreamFactory(func(r forgo.Resolver, _ buildEnv) (ClientStreamHandler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(stream grpc.ClientStreamingServer[Req, Resp]) error {
			return fn(service, stream)
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
func BidiStreamFor[S, Req, Resp any](fn func(S, grpc.BidiStreamingServer[Req, Resp]) error) HandlerFactory {
	return bidiStreamFactory(func(r forgo.Resolver, _ buildEnv) (BidiStreamHandler[Req, Resp], error) {
		service, err := r.Resolve[S]()
		if err != nil {
			return nil, err
		}
		return func(stream grpc.BidiStreamingServer[Req, Resp]) error {
			return fn(service, stream)
		}, nil
	})
}

// BidiStreamAs serves a bidi-streaming method with the BidiStreamHandler that constructor returns.
func BidiStreamAs[Req, Resp any](constructor any) HandlerFactory {
	return bidiStreamFactory(func(r forgo.Resolver, _ buildEnv) (BidiStreamHandler[Req, Resp], error) {
		return r.Call[BidiStreamHandler[Req, Resp]](constructor)
	})
}

// newFactory returns a HandlerFactory of kind for the messages Req and Resp.
func newFactory[Req, Resp any](kind methodKind, build func(forgo.Resolver, buildEnv) (handlerWrapper, error)) HandlerFactory {
	return HandlerFactory{
		build: build,
		kind:  kind,
		req:   reflect.TypeFor[Req](),
		resp:  reflect.TypeFor[Resp](),
		newRequest: func() proto.Message {
			// New checked that *Req is a proto.Message.
			return any(new(Req)).(proto.Message)
		},
	}
}

func unaryFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (Handler[Req, Resp], error)) HandlerFactory {
	return newFactory[Req, Resp](kindUnary, func(r forgo.Resolver, env buildEnv) (handlerWrapper, error) {
		fn, err := resolve(r, env)
		if err != nil {
			return nil, err
		}
		return unaryTerminal(fn), nil
	})
}

func serverStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (ServerStreamHandler[Req, Resp], error)) HandlerFactory {
	return newFactory[Req, Resp](kindServerStream, func(r forgo.Resolver, env buildEnv) (handlerWrapper, error) {
		fn, err := resolve(r, env)
		if err != nil {
			return nil, err
		}
		return serverStreamTerminal(fn), nil
	})
}

func clientStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (ClientStreamHandler[Req, Resp], error)) HandlerFactory {
	return newFactory[Req, Resp](kindClientStream, func(r forgo.Resolver, env buildEnv) (handlerWrapper, error) {
		fn, err := resolve(r, env)
		if err != nil {
			return nil, err
		}
		return func(validate func(proto.Message) error) operation.Handler {
			return func(op *operation.Context) error {
				st := op.State.(*State)
				stream := &opStream{ServerStream: st.stream, op: op, st: st, validate: validate, keepResponse: true}
				return st.result(fn(&grpc.GenericServerStream[Req, Resp]{ServerStream: stream}))
			}
		}, nil
	})
}

func bidiStreamFactory[Req, Resp any](resolve func(forgo.Resolver, buildEnv) (BidiStreamHandler[Req, Resp], error)) HandlerFactory {
	return newFactory[Req, Resp](kindBidiStream, func(r forgo.Resolver, env buildEnv) (handlerWrapper, error) {
		fn, err := resolve(r, env)
		if err != nil {
			return nil, err
		}
		return func(validate func(proto.Message) error) operation.Handler {
			return func(op *operation.Context) error {
				st := op.State.(*State)
				stream := &opStream{ServerStream: st.stream, op: op, st: st, validate: validate}
				return st.result(fn(&grpc.GenericServerStream[Req, Resp]{ServerStream: stream}))
			}
		}, nil
	})
}

// unaryTerminal builds the terminal handler of a unary method: it validates the request the server
// received, calls fn and sends its response inside the chain, so that the Recorder logs a failed send.
func unaryTerminal[Req, Resp any](fn Handler[Req, Resp]) handlerWrapper {
	return func(validate func(proto.Message) error) operation.Handler {
		return func(op *operation.Context) error {
			st := op.State.(*State)
			if f := st.failure.Load(); f != nil {
				return f.err
			}
			if err := validateMessage(validate, st.Request); err != nil {
				return err
			}
			resp, err := fn(op, any(st.Request).(*Req))
			if err != nil {
				return err
			}
			if resp == nil {
				return errNilResponse
			}
			msg := any(resp).(proto.Message)
			st.Response = msg
			if err := st.stream.SendMsg(msg); err != nil {
				return st.fail(send, err)
			}
			return nil
		}
	}
}

// serverStreamTerminal builds the terminal handler of a server-streaming method: it validates the request
// the server received and calls fn.
func serverStreamTerminal[Req, Resp any](fn ServerStreamHandler[Req, Resp]) handlerWrapper {
	return func(validate func(proto.Message) error) operation.Handler {
		return func(op *operation.Context) error {
			st := op.State.(*State)
			if f := st.failure.Load(); f != nil {
				return f.err
			}
			if err := validateMessage(validate, st.Request); err != nil {
				return err
			}
			stream := &opStream{ServerStream: st.stream, op: op, st: st}
			return st.result(fn(any(st.Request).(*Req), &grpc.GenericServerStream[Req, Resp]{ServerStream: stream}))
		}
	}
}
