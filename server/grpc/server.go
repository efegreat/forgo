package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"syscall"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ServerNameGRPC is the server_name of the gRPC server's logs.
const ServerNameGRPC = "grpc"

// server serves the services of a Config with grpc-go. Config.New creates it.
type server struct {
	cfg Config
	g   *grpc.Server
	// lg is the server's logger, which Init stores, and every call's context carries.
	lg zerolog.Logger
	// terminating is closed when Terminate starts, for State.Terminating.
	terminating   chan struct{}
	terminateOnce sync.Once
	// validates reports whether the default Validator checks anything on a request type, nil with a
	// Validator of the configuration's own.
	validates func(reflect.Type) bool
}

// Term returns the termination settings of the Config.
func (s *server) Term() forgo.TermConfig {
	return s.cfg.Term
}

// Name returns ServerNameGRPC.
func (s *server) Name() string {
	return ServerNameGRPC
}

// Init stores the logger of ctx, which the context of every call carries.
func (s *server) Init(ctx context.Context) error {
	s.lg = *logger.FromCtx(ctx)
	return nil
}

// Serve listens on Config.Addr and serves until Terminate or Kill, then returns nil. It serves TLS when
// Config.TLSConfig is set. After Terminate or Kill, it returns at once.
func (s *server) Serve() error {
	ln, err := net.Listen(s.cfg.Network, s.cfg.Addr)
	if err != nil {
		return err
	}
	err = s.g.Serve(s.listener(ln))
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// listener returns ln, reporting to Config.Observer. grpc-go sets TCP_USER_TIMEOUT only on a *net.TCPConn,
// which a connection wrapped for an IConnIOObserver is not, so on Linux the listener sets it first, as grpc-go
// would.
func (s *server) listener(ln net.Listener) net.Listener {
	o := s.observer()
	if _, wraps := o.(transport.IConnIOObserver); wraps && userTimeoutSupported {
		if d, ok := userTimeout(s.cfg.Keepalive); ok {
			ln = &userTimeoutListener{Listener: ln, timeout: d, observer: o}
		}
	}
	return &transport.Listener{Listener: ln, Observer: o}
}

// observer returns Config.Observer. An IConnIOObserver is wrapped in a drainObserver.
func (s *server) observer() transport.IConnObserver {
	if o, ok := s.cfg.Observer.(transport.IConnIOObserver); ok {
		return drainObserver{IConnIOObserver: o, terminating: s.terminating}
	}
	return s.cfg.Observer
}

// drainObserver is not told about the connections clients reset once the server terminates. A grpc-go
// client closes an idle connection as soon as it reads the GOAWAY of GracefulStop, before the PING that
// follows it, and its system resets a connection closed with unread data, so every graceful stop with idle
// clients would report failures.
type drainObserver struct {
	transport.IConnIOObserver
	terminating <-chan struct{}
}

func (o drainObserver) OnReadError(c net.Conn, err error) {
	if !o.drainReset(err) {
		o.IConnIOObserver.OnReadError(c, err)
	}
}

func (o drainObserver) OnWriteError(c net.Conn, err error) {
	if !o.drainReset(err) {
		o.IConnIOObserver.OnWriteError(c, err)
	}
}

// drainReset reports whether err is a reset by the client, or a write to a connection it reset, after
// termination started.
func (o drainObserver) drainReset(err error) bool {
	select {
	case <-o.terminating:
		return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
	default:
		return false
	}
}

// Terminate closes State.Terminating, stops accepting connections and calls, and waits for the calls in
// flight to end: grpc-go's GracefulStop waits for every handler, even one whose client went away. When ctx
// ends first, it returns ctx's error, for App to Kill the server.
func (s *server) Terminate(ctx context.Context) error {
	s.terminateOnce.Do(func() { close(s.terminating) })
	stopped := make(chan struct{})
	go func() {
		s.g.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("grpc: graceful stop: %w", ctx.Err())
	}
}

// Kill closes the listener and every connection, so the calls still in flight see their context canceled
// and their sends fail. App calls it when Terminate failed or timed out. It does not wait for the handlers.
func (s *server) Kill() {
	s.g.Stop()
}

// chain builds the chain of a method once: the base middlewares, the server's, then middlewares.
func (s *server) chain(handler operation.Handler, middlewares []operation.Middleware) operation.Handler {
	var chain []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		chain = append(chain,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Recorder(
				recorderDisabled,
				RequestLogFields(RequestRecorderBodyLimit, RequestRecorderHeaderLimit, s.cfg.SensitiveFields),
				ResponseLogFields(ResponseRecorderBodyLimit, s.cfg.SensitiveFields),
			),
			// Sends the response, or answers the error, the *middleware.PanicError of Recovery included.
			Responder(s.cfg.ErrorMapper, s.cfg.ErrorDomain),
			middleware.Recovery(nil),
		)
	}
	chain = append(chain, s.cfg.ResolvedMiddlewares()...)
	chain = append(chain, middlewares...)
	return operation.Chain(handler, chain...)
}

// streamHandler builds the chain of a method once, and returns the grpc.StreamHandler that runs it for each
// call. The request of a unary or server-streaming method is received before the chain, so that middlewares
// and the Recorder can read it. A failure to receive it is answered by grpc-go, and returned by the terminal
// handler. The terminal of a server-streaming method receives the request again, from a replayStream below
// the wrappers of the stream.
func (s *server) streamHandler(m resolvedMethod) grpc.StreamHandler {
	handler := s.chain(m.handler, m.middlewares)
	receivesFirst := m.kind.receivesFirst()
	replays := m.kind == kindServerStream
	return func(_ any, ss grpc.ServerStream) error {
		c := &call{st: State{FullMethod: m.fullMethod, kind: m.kind, stream: ss, terminating: s.terminating}}
		if receivesFirst {
			req := m.newRequest()
			if err := ss.RecvMsg(req); err != nil {
				c.st.fail(receive, err)
			} else {
				c.st.Request = req
			}
		}
		if replays {
			c.replay = replayStream{ServerStream: ss, st: &c.st}
			c.st.stream = &c.replay
		}
		return s.run(handler, c, m.name, m.args)
	}
}

// unknownHandler builds the chain of the calls no configured method matches once, and returns the
// grpc.StreamHandler that runs it for each such call. It runs as the "unimplemented" operation, through the
// base and server middlewares, and is answered Unimplemented. Its kind is unknown, so it is taken for a bidi
// stream, which has no response for the Responder to send.
func (s *server) unknownHandler() grpc.StreamHandler {
	handler := s.chain(func(op *operation.Context) error {
		return errUnimplemented.Format(op.State.(*State).FullMethod)
	}, nil)
	return func(_ any, ss grpc.ServerStream) error {
		fullMethod, _ := grpc.MethodFromServerStream(ss)
		c := &call{st: State{FullMethod: fullMethod, kind: kindBidiStream, stream: ss, terminating: s.terminating}}
		return s.run(handler, c, unimplementedName, nil)
	}
}

// call holds the values the server allocates for each call, in one allocation: its operation, the
// operation's State and its Args, and the replayStream of a server-streaming method.
type call struct {
	op     operation.Context
	st     State
	args   types.Map[any, any]
	replay replayStream
}

// run runs the chain of a call, sets the operation ID in the trailer and returns the status that answers
// the call. c holds the call's State.
func (s *server) run(handler operation.Handler, c *call, name string, args map[any]any) error {
	st := &c.st
	ctx := st.stream.Context()
	c.args.ResetCopyOnWrite(args)
	c.op = operation.Context{
		Context: logger.NewCtx(ctx, s.lg),
		ID:      firstValue(metadata.ValueFromIncomingContext(ctx, ReqIDMetadata)),
		Name:    name,
		Type:    OperationTypeGRPCHandler,
		Args:    &c.args,
		State:   st,
	}
	op := &c.op
	// Without base middlewares nothing answered the call yet. With them, the Responder did, and respond does
	// nothing.
	err := respond(op, s.cfg.ErrorMapper, s.cfg.ErrorDomain, handler(op))
	if op.ID != "" {
		st.stream.SetTrailer(metadata.MD{ReqIDMetadata: {op.ID}})
	}
	if err == nil {
		return nil
	}
	if f := st.failure.Load(); f != nil {
		// grpc-go sent it already, unless it is the failure of a stream a middleware wrapped.
		return f.status.Err()
	}
	return st.answer.Err()
}

// GetServiceInfo returns the services the server serves, for the reflection service. It is called only
// once the server serves.
func (s *server) GetServiceInfo() map[string]grpc.ServiceInfo {
	return s.g.GetServiceInfo()
}

func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
