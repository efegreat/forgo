package grpc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/transport"

	"github.com/go-playground/validator/v10"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const (
	defaultNetwork        = "tcp"
	defaultAddr           = ":9090"
	defaultMaxRecvMsgSize = 4 << 20 // grpc-go's default
	noMsgSizeLimit        = math.MaxInt32
)

// Config declares a gRPC server and the services it serves. It is a forgo.ServerFactory, so it can be
// declared as a package variable and passed to forgo.WithServers. Zero fields get the defaults of Sanitize.
//
// The server serves every configured method itself, through a chain built once: the handlers of the
// generated service descriptors never run, and the service types do not have to implement the generated
// interfaces. Calls to any other method are answered Unimplemented.
type Config struct {
	forgo.ServerConfig
	// Network is the network the server listens on, such as "tcp" or "unix". Default: "tcp".
	Network string
	// Addr is the address the server listens on. Default: ":9090".
	Addr string
	// TLSConfig serves TLS when set. Its Certificates, GetCertificate or GetConfigForClient provide the
	// certificates.
	TLSConfig *tls.Config
	// MaxRecvMsgSize bounds the request messages, in bytes. A larger one is answered ResourceExhausted, and
	// logged with errCode "message_too_large". Default: 4 MiB. Negative means no limit.
	MaxRecvMsgSize int
	// MaxSendMsgSize bounds the response messages, in bytes. Sending a larger one fails, and the call is
	// answered ResourceExhausted. Zero or negative means no limit.
	MaxSendMsgSize int
	// MaxConcurrentStreams bounds the calls in flight on one connection. Zero means no limit.
	MaxConcurrentStreams uint32
	// ConnectionTimeout bounds the setup of a connection, TLS handshake included. Zero means grpc-go's
	// default of 120 seconds.
	ConnectionTimeout time.Duration
	// Keepalive and KeepalivePolicy are passed to grpc-go as they are. Their zero values are grpc-go's
	// defaults.
	Keepalive       keepalive.ServerParameters
	KeepalivePolicy keepalive.EnforcementPolicy
	// InitialWindowSize and InitialConnWindowSize set the flow control windows of streams and of
	// connections, in bytes. Setting either turns off the dynamic window that grpc-go sizes from the
	// bandwidth-delay product. Zero means that default.
	InitialWindowSize     int32
	InitialConnWindowSize int32
	// MaxHeaderListSize bounds the headers of a request, in bytes. Zero means grpc-go's default of 16 MiB.
	MaxHeaderListSize uint32
	// StatsHandlers are told about connections and calls, such as otelgrpc's tracing and metrics handler.
	StatsHandlers []stats.Handler
	// Observer is told about failed accepts, and about failed reads and writes when it also implements
	// transport.IConnIOObserver. Such an observer wraps every connection, which grpc-go then no longer sets
	// TCP_USER_TIMEOUT on, so on Linux the server sets it itself, as grpc-go would from Keepalive.
	Observer transport.IConnObserver
	// Validators are the custom validation tags of the default Validator, by tag name, like those of
	// http.Config, so that request structs shared with an HTTP server validate the same way.
	Validators map[string]validator.Func
	// Validator validates every request before the handler gets it: the request of a unary or
	// server-streaming method, and every request of a client or bidi stream. It gets a pointer to the
	// handler's request type. Its error is answered InvalidArgument with errCode "validation" by
	// MapValidation. Default: go-playground/validator on the validate tags of a struct, as over HTTP, then
	// the Validate() error method of a request that has one, such as a proto message protoc-gen-validate
	// generates. Use a function that returns nil to turn validation off.
	Validator func(any) error
	// ErrorMapper turns the errors of handlers and middlewares into the operation.Error clients get. Setting it
	// replaces the default mappers: MapValidation, MapStatus and MapContext.
	ErrorMapper *operation.ErrorMapper
	// ErrorDomain is the Domain of the errdetails.ErrorInfo sent with every error, whose Reason is the error's
	// Code, such as the service's DNS name.
	ErrorDomain string
	// SensitiveFields names the fields whose values are masked as [*] in logs: in request and response metadata
	// and in request and response messages, by their proto field names, at any depth. Names match
	// case-insensitively. The credential metadata authorization, proxy-authorization, cookie, set-cookie
	// and x-api-key are always masked.
	SensitiveFields []string
	// LogStreamMessages logs the first LogStreamMessages messages of each streamed direction of a call, each on a
	// line of its own, masked like the request and response the Recorder logs: those a client or bidi stream
	// receives, and those a server or bidi stream sends. A negative number logs every message. Zero, the
	// default, logs none, since a long-lived stream would log every message it carries.
	LogStreamMessages int
	// Reflection serves the gRPC server reflection service, v1 and v1alpha, for tools such as grpcurl. Its
	// calls run through the base and server middlewares like any other.
	Reflection bool
	// Services are the services the server serves.
	Services []ServiceConfig
}

// Sanitize returns a copy of c with defaults applied. The default Validator is built by New, because
// building it fails on invalid Validators.
func (c Config) Sanitize() Config {
	c.ServerConfig = c.ServerConfig.Sanitize()
	if c.Network == "" {
		c.Network = defaultNetwork
	}
	if c.Addr == "" {
		c.Addr = defaultAddr
	}
	switch {
	case c.MaxRecvMsgSize == 0:
		c.MaxRecvMsgSize = defaultMaxRecvMsgSize
	case c.MaxRecvMsgSize < 0:
		c.MaxRecvMsgSize = noMsgSizeLimit
	}
	if c.MaxSendMsgSize <= 0 {
		c.MaxSendMsgSize = noMsgSizeLimit
	}
	if c.ErrorMapper == nil {
		c.ErrorMapper = newDefaultErrorMapper()
	}
	return c
}

// Name returns ServerNameGRPC.
func (c Config) Name() string {
	return ServerNameGRPC
}

// New resolves the middlewares and the handler of every method, checks every method against its service
// descriptor, and its handler's types against the method's messages, builds every method's chain and
// registers it, so that a configuration error fails at startup with the service and method it is about.
func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	cfg := c.Sanitize()
	var validates func(reflect.Type) bool
	if cfg.Validator == nil {
		validate, check, err := newDefaultValidator(cfg.Validators)
		if err != nil {
			return nil, err
		}
		cfg.Validator, validates = validate, check
	}
	observer, err := r.Resolve[operation.IObserver]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}
	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc

	s := &server{cfg: cfg, observer: observer, terminating: make(chan struct{}), validates: validates}
	services := cfg.Services
	if cfg.Reflection {
		services = append(slices.Clip(services), reflectionServices(s)...)
	}
	descs, err := s.services(r, services)
	if err != nil {
		return nil, err
	}

	s.g = grpc.NewServer(cfg.serverOptions(s.unknownHandler())...)
	for _, desc := range descs {
		s.g.RegisterService(desc, nil)
	}
	return s, nil
}

// serverOptions returns the grpc-go options of the configuration.
func (c Config) serverOptions(unknown grpc.StreamHandler) []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(c.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(c.MaxSendMsgSize),
		grpc.KeepaliveParams(c.Keepalive),
		grpc.KeepaliveEnforcementPolicy(c.KeepalivePolicy),
		grpc.UnknownServiceHandler(unknown),
		grpc.ForceServerCodecV2(newCodec()),
	}
	if c.TLSConfig != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(c.TLSConfig)))
	}
	if c.MaxConcurrentStreams > 0 {
		opts = append(opts, grpc.MaxConcurrentStreams(c.MaxConcurrentStreams))
	}
	if c.ConnectionTimeout > 0 {
		opts = append(opts, grpc.ConnectionTimeout(c.ConnectionTimeout))
	}
	if c.InitialWindowSize > 0 {
		opts = append(opts, grpc.InitialWindowSize(c.InitialWindowSize))
	}
	if c.InitialConnWindowSize > 0 {
		opts = append(opts, grpc.InitialConnWindowSize(c.InitialConnWindowSize))
	}
	if c.MaxHeaderListSize > 0 {
		opts = append(opts, grpc.MaxHeaderListSize(c.MaxHeaderListSize))
	}
	for _, h := range c.StatsHandlers {
		if h != nil {
			opts = append(opts, grpc.StatsHandler(h))
		}
	}
	return opts
}

// ServiceConfig declares a service: its generated descriptor, and the methods the server serves.
type ServiceConfig struct {
	// Desc is the generated service descriptor, such as &userpb.UserService_ServiceDesc. Required. Only its
	// ServiceName, Metadata and method kinds are used: its handlers never run.
	Desc *grpc.ServiceDesc
	// Middlewares run on every method of the service, after the base and server middlewares and before the
	// method's.
	Middlewares []forgo.MiddlewareFactory
	// Args are static values in the Args of every call of the service. A method's Args override them.
	Args map[any]any
	// Methods are the methods the server serves. Calls to the other methods of the service are answered
	// Unimplemented.
	Methods []MethodConfig
}

// MethodConfig declares a method of a service and the handler that serves it.
type MethodConfig struct {
	// Method is the method's name in the .proto file, such as "GetUser". Required.
	Method string
	// Name is the operation name of the method's calls, logged as op_name. Default: the full method, such
	// as "/user.v1.UserService/GetUser".
	Name string
	// Middlewares run on the method's calls, after the service's.
	Middlewares []forgo.MiddlewareFactory
	// Handler serves the method's calls. Required. Its kind and message types must match the method's.
	Handler HandlerFactory
	// Args are static values in the Args of every call of the method, such as ArgKeyRecorderDisabled.
	Args map[any]any
}

// resolvedMethod is a method whose handler, middlewares and Args are resolved.
type resolvedMethod struct {
	name        string // op_name
	fullMethod  string
	kind        methodKind
	newRequest  func() proto.Message
	middlewares []operation.Middleware // service's, then method's
	handler     operation.Handler      // the terminal handler
	args        map[any]any
}

// services resolves every service, and returns the descriptors that serve their methods. Errors name the
// service and the method.
func (s *server) services(r forgo.Resolver, services []ServiceConfig) ([]*grpc.ServiceDesc, error) {
	env := buildEnv{
		termSig:   s.cfg.Term.TermSig,
		validate:  s.cfg.Validator,
		validates: s.validates,
		sensitive: newSensitiveFields(s.cfg.SensitiveFields),
	}
	for i, sc := range services {
		if sc.Desc == nil {
			return nil, fmt.Errorf("grpc: service %d: no Desc", i)
		}
		name := sc.Desc.ServiceName
		// grpc-go exits the process on a service registered twice.
		if slices.Contains(env.services, name) {
			return nil, fmt.Errorf("grpc: service %q: configured twice", name)
		}
		env.services = append(env.services, name)
	}

	descs := make([]*grpc.ServiceDesc, len(services))
	for i, sc := range services {
		desc, err := s.service(r, env, sc)
		if err != nil {
			return nil, err
		}
		descs[i] = desc
	}
	return descs, nil
}

// service resolves the methods of sc, and returns the descriptor that serves them.
func (s *server) service(r forgo.Resolver, env buildEnv, sc ServiceConfig) (*grpc.ServiceDesc, error) {
	name := sc.Desc.ServiceName
	if len(sc.Methods) == 0 {
		return nil, fmt.Errorf("grpc: service %q: no Methods", name)
	}
	middlewares, err := forgo.ResolveMiddlewares(r, sc.Middlewares)
	if err != nil {
		return nil, fmt.Errorf("grpc: service %q: %w", name, err)
	}
	// Generated code registers its descriptors, which tells the message types of every method.
	var sd protoreflect.ServiceDescriptor
	if d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name)); err == nil {
		sd, _ = d.(protoreflect.ServiceDescriptor)
	}

	desc := &grpc.ServiceDesc{
		ServiceName: name,
		Metadata:    sc.Desc.Metadata, // the .proto file, for reflection
		Streams:     make([]grpc.StreamDesc, 0, len(sc.Methods)),
	}
	for _, mc := range sc.Methods {
		if slices.ContainsFunc(desc.Streams, func(d grpc.StreamDesc) bool { return d.StreamName == mc.Method }) {
			return nil, fmt.Errorf("grpc: service %q method %q: configured twice", name, mc.Method)
		}
		m, err := s.method(r, env, sc, sd, middlewares, mc)
		if err != nil {
			return nil, fmt.Errorf("grpc: service %q method %q: %w", name, mc.Method, err)
		}
		// Unary methods are registered as streams too, so that the Responder sends their response inside
		// the chain, and a failed send is logged. grpc-go still enforces one message each way from the flags.
		desc.Streams = append(desc.Streams, grpc.StreamDesc{
			StreamName:    mc.Method,
			Handler:       s.streamHandler(m),
			ServerStreams: m.kind.serverStreams(),
			ClientStreams: m.kind.clientStreams(),
		})
	}
	return desc, nil
}

// method checks mc against the service's descriptors and resolves it.
func (s *server) method(
	r forgo.Resolver,
	env buildEnv,
	sc ServiceConfig,
	sd protoreflect.ServiceDescriptor,
	serviceMiddlewares []operation.Middleware,
	mc MethodConfig,
) (resolvedMethod, error) {
	if mc.Method == "" {
		return resolvedMethod{}, errors.New("no Method")
	}
	h := mc.Handler
	if h.build == nil {
		return resolvedMethod{}, errors.New("no Handler")
	}
	kind, ok := descKind(sc.Desc, mc.Method)
	if !ok {
		return resolvedMethod{}, errors.New("the service has no such method")
	}
	if kind != h.kind {
		return resolvedMethod{}, fmt.Errorf("the method is %s, but the handler is %s", kind, h.kind)
	}
	mm, err := methodMessagesOf(sd, mc.Method, kind)
	if err != nil {
		return resolvedMethod{}, err
	}

	built, err := h.build(r, env, mm)
	if err != nil {
		return resolvedMethod{}, err
	}
	middlewares, err := forgo.ResolveMiddlewares(r, mc.Middlewares)
	if err != nil {
		return resolvedMethod{}, err
	}
	// shared by every call until one writes to its Args
	var args map[any]any
	if len(sc.Args) > 0 || len(mc.Args) > 0 {
		args = maps.Clone(sc.Args)
		if args == nil {
			args = make(map[any]any, len(mc.Args))
		}
		maps.Copy(args, mc.Args)
	}

	fullMethod := "/" + sc.Desc.ServiceName + "/" + mc.Method
	name := mc.Name
	if name == "" {
		name = fullMethod
	}
	return resolvedMethod{
		name:        name,
		fullMethod:  fullMethod,
		kind:        kind,
		newRequest:  built.newRequest,
		middlewares: slices.Concat(serviceMiddlewares, middlewares),
		handler:     built.handler,
		args:        args,
	}, nil
}

// descKind returns the kind of the method of desc, and false when desc has no such method.
func descKind(desc *grpc.ServiceDesc, method string) (methodKind, bool) {
	for _, m := range desc.Methods {
		if m.MethodName == method {
			return kindUnary, true
		}
	}
	for _, sd := range desc.Streams {
		if sd.StreamName == method {
			return streamKind(sd.ClientStreams, sd.ServerStreams), true
		}
	}
	return 0, false
}

// methodMessagesOf returns the messages of the method of sd, the registered descriptor of the service,
// which must be of kind. It returns no messages when sd is nil.
func methodMessagesOf(sd protoreflect.ServiceDescriptor, method string, kind methodKind) (methodMessages, error) {
	if sd == nil {
		return methodMessages{}, nil
	}
	md := sd.Methods().ByName(protoreflect.Name(method))
	if md == nil {
		return methodMessages{}, fmt.Errorf("the registered descriptor of the service has no method %q", method)
	}
	if got := streamKind(md.IsStreamingClient(), md.IsStreamingServer()); got != kind {
		return methodMessages{}, fmt.Errorf("the method is %s in its .proto file, but the handler is %s", got, kind)
	}
	return methodMessages{input: md.Input(), output: md.Output()}, nil
}
