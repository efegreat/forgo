package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var errNilInterceptor = errors.New("grpc: nil interceptor")

// UnaryInterceptor runs a grpc-go unary interceptor, such as one of go-grpc-middleware's, as a middleware,
// at its place in the chain. It runs on unary methods only, like in grpc-go, and the other calls go on to
// next. Its request is State.Request, and the response it returns becomes State.Response, so it may answer
// without calling its handler. The context its handler gets becomes the operation's context until the
// handler returns. Its UnaryServerInfo has no Server, since forgo serves no generated service.
func UnaryInterceptor(interceptor grpc.UnaryServerInterceptor) forgo.MiddlewareFactory {
	return func(forgo.Resolver) (operation.Middleware, error) {
		if interceptor == nil {
			return nil, errNilInterceptor
		}
		return unaryMiddleware(interceptor), nil
	}
}

// UnaryInterceptorAs runs the grpc.UnaryServerInterceptor that constructor returns as UnaryInterceptor
// does. The constructor's parameters are resolved from the bootstrap, once, in New.
func UnaryInterceptorAs(constructor any) forgo.MiddlewareFactory {
	return func(r forgo.Resolver) (operation.Middleware, error) {
		interceptor, err := r.Call[grpc.UnaryServerInterceptor](constructor)
		if err != nil {
			return nil, err
		}
		return unaryMiddleware(interceptor), nil
	}
}

// StreamInterceptor runs a grpc-go stream interceptor as a middleware, at its place in the chain. It runs on
// streaming methods only, like in grpc-go, and the other calls go on to next. The stream its handler gets
// becomes the one the rest of the chain receives and sends on until the handler returns, and so does its
// context. The request of a server-streaming method is received before the chain, so the interceptor's
// stream never sees it. Its handler gets no Server, since forgo serves no generated service.
func StreamInterceptor(interceptor grpc.StreamServerInterceptor) forgo.MiddlewareFactory {
	return func(forgo.Resolver) (operation.Middleware, error) {
		if interceptor == nil {
			return nil, errNilInterceptor
		}
		return streamMiddleware(interceptor), nil
	}
}

// StreamInterceptorAs runs the grpc.StreamServerInterceptor that constructor returns as StreamInterceptor
// does. The constructor's parameters are resolved from the bootstrap, once, in New.
func StreamInterceptorAs(constructor any) forgo.MiddlewareFactory {
	return func(r forgo.Resolver) (operation.Middleware, error) {
		interceptor, err := r.Call[grpc.StreamServerInterceptor](constructor)
		if err != nil {
			return nil, err
		}
		return streamMiddleware(interceptor), nil
	}
}

func unaryMiddleware(interceptor grpc.UnaryServerInterceptor) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(op *operation.Context) error {
			st := op.State.(*State)
			if st.kind != kindUnary {
				return next(op)
			}
			// The interceptor gets the operation's own context, so that the one it passes on never holds the
			// operation, which would then look itself up forever.
			outer := op.Context
			info := &grpc.UnaryServerInfo{FullMethod: st.FullMethod}
			resp, err := interceptor(outer, st.Request, info, func(ctx context.Context, req any) (any, error) {
				msg, ok := req.(proto.Message)
				if !ok {
					return nil, fmt.Errorf("grpc: the interceptor passed a request of type %T", req)
				}
				st.Request = msg
				op.Context = ctx
				defer func() { op.Context = outer }()
				if err := next(op); err != nil {
					return nil, err
				}
				return st.Response, nil
			})
			if err != nil {
				return err
			}
			if resp == nil {
				st.Response = nil
				return nil
			}
			msg, ok := resp.(proto.Message)
			if !ok {
				return fmt.Errorf("grpc: the interceptor returned a response of type %T", resp)
			}
			st.Response = msg
			return nil
		}
	}
}

func streamMiddleware(interceptor grpc.StreamServerInterceptor) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(op *operation.Context) error {
			st := op.State.(*State)
			if st.kind == kindUnary {
				return next(op)
			}
			outer, stream := op.Context, st.stream
			info := &grpc.StreamServerInfo{
				FullMethod:     st.FullMethod,
				IsClientStream: st.kind.clientStreams(),
				IsServerStream: st.kind.serverStreams(),
			}
			return interceptor(nil, contextStream{ServerStream: stream, ctx: outer}, info, func(_ any, ss grpc.ServerStream) error {
				op.Context, st.stream = ss.Context(), ss
				defer func() { op.Context, st.stream = outer, stream }()
				return next(op)
			})
		}
	}
}

// contextStream is a stream whose Context is the operation's own context, which carries the server's
// logger, so that a context derived from it does too.
type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s contextStream) Context() context.Context {
	return s.ctx
}
