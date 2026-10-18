package forgotest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/process"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

// defaultTermTimeout is the TermTimeout of servers whose Term returns zero or less, as in forgo.App.
const defaultTermTimeout = 30 * time.Second

var (
	// ErrNotFound is returned by Env.Addr when the env serves no server of the name.
	ErrNotFound = errors.New("forgotest: not found")
	// ErrAmbiguous is returned by Env.Addr when the env serves more than one server of the name, such as
	// two HTTP servers.
	ErrAmbiguous = errors.New("forgotest: ambiguous")
)

// IListenerServer is a server that serves on a listener it is given, such as the HTTP and gRPC servers. An
// Env serves it on a loopback listener of its own, whatever address its configuration names, and tests
// reach it through Env.Addr. Implement it on a custom server to have it served in tests too.
type IListenerServer interface {
	forgo.IServer
	// ServeListener serves on ln as Serve does on a listener of its own: until Terminate or Kill, then it
	// returns nil. It closes ln.
	ServeListener(ln net.Listener) error
}

// Env runs the servers of an app for the tests of a package. Create it with New and tear it down with
// Close. Unlike forgo.App.Run, it catches no signal, serves only the IListenerServer servers, on loopback,
// and does not sleep for TermSig.Wait when it terminates them. It is safe for concurrent use.
type Env struct {
	boot *forgo.Bootstrap
	// served are the servers the env serves, in the order of the app's servers.
	served []*servedServer

	closeOnce sync.Once
}

// servedServer is a server the env serves on a loopback listener.
type servedServer struct {
	IListenerServer
	lg   zerolog.Logger
	addr string
	// served is closed when ServeListener returned, and err holds its error.
	served chan struct{}
	err    error
}

// Option configures an Env.
type Option func(*options)

type options struct {
	logger *logger.Config
}

// WithLogger sets the configuration of the env's logger, for example to raise its level or to read the logs
// from a buffer through Output. Without it, the env logs like the app, but to os.Stderr and to no file, so
// tests write no log files, and go test shows the logs of a package that passes only with -v.
func WithLogger(cfg logger.Config) Option {
	return func(o *options) {
		o.logger = &cfg
	}
}

// New builds the bootstrap of app's definition, creates and initializes every server in order with a
// logger that carries app_name, app_id, server_name and server_id, seals the bootstrap, and serves the
// IListenerServer servers on loopback listeners, as forgo.App.Run does before it serves. A failed New
// terminates the servers it served and closes the resources it created.
//
// Unlike Run, New does not install its logger as the default one (logger.SetDefault), so two envs in one
// test binary do not swap each other's.
func New(app *forgo.App, opts ...Option) (*Env, error) {
	if app == nil {
		return nil, errors.New("forgotest: nil app")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	def := app.Definition()
	if err := validate(def.Servers); err != nil {
		return nil, err
	}

	cfg := def.Logger
	cfg.Output = os.Stderr
	cfg.File = logger.FileConfig{}
	if o.logger != nil {
		cfg = *o.logger
	}
	lg := cfg.New().
		With().
		Str(logger.AppNameLogKey, def.Name).
		Str(logger.AppIDLogKey, xid.New().String()).
		Logger()

	boot := forgo.NewBootstrap(def.Resources, def.Providers)
	resolver, err := boot.Build()
	if err != nil {
		return nil, err
	}
	env := &Env{boot: boot}
	if err := env.start(lg, resolver, def.Servers); err != nil {
		return nil, errors.Join(err, env.Close())
	}
	return env, nil
}

// validate checks the server list before anything is bootstrapped, as forgo.App.Run does.
func validate(servers []forgo.ServerFactory) error {
	if len(servers) == 0 {
		return errors.New("forgotest: no servers")
	}
	for i, sf := range servers {
		if isNil(sf) {
			return fmt.Errorf("forgotest: server %d is nil", i)
		}
	}
	return nil
}

// start creates and initializes every server, seals the bootstrap, then serves the IListenerServer servers
// on loopback listeners.
func (e *Env) start(lg zerolog.Logger, r forgo.Resolver, factories []forgo.ServerFactory) error {
	var listeners []*servedServer
	for _, sf := range factories {
		s, err := sf.New(r)
		if err != nil {
			return fmt.Errorf("forgotest: server %q: new: %w", sf.Name(), err)
		}
		if isNil(s) {
			return fmt.Errorf("forgotest: server %q: New returned nil", sf.Name())
		}

		sLg := lg.With().
			Str(logger.ServerIDLogKey, xid.New().String()).
			Str(logger.ServerNameLogKey, s.Name()).
			Logger()
		if err := s.Init(logger.NewCtx(context.Background(), sLg)); err != nil {
			return fmt.Errorf("forgotest: server %q: init: %w", s.Name(), err)
		}
		if ls, ok := s.(IListenerServer); ok {
			listeners = append(listeners, &servedServer{IListenerServer: ls, lg: sLg, served: make(chan struct{})})
		}
	}

	if err := e.boot.Seal(); err != nil {
		return err
	}

	for _, s := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return fmt.Errorf("forgotest: server %q: listen: %w", s.Name(), err)
		}
		s.addr = ln.Addr().String()
		e.served = append(e.served, s)
		go s.serve(ln)
	}
	return nil
}

// Addr returns the host:port of the loopback listener the env serves the server named name on, such as
// http.ServerNameHTTP or grpc.ServerNameGRPC. It fails with ErrNotFound when the env serves no server of
// that name, as for servers that do not implement IListenerServer, and with ErrAmbiguous when it serves
// more than one.
func (e *Env) Addr(name string) (string, error) {
	var addrs []string
	for _, s := range e.served {
		if s.Name() == name {
			addrs = append(addrs, s.addr)
		}
	}
	switch len(addrs) {
	case 1:
		return addrs[0], nil
	case 0:
		return "", fmt.Errorf("%w: no served server is named %q", ErrNotFound, name)
	default:
		return "", fmt.Errorf("%w: %d served servers are named %q", ErrAmbiguous, len(addrs), name)
	}
}

// Close terminates the served servers in parallel, waits for the goroutines started with process.Go, then
// closes the resources in reverse order, and returns the failures joined: the errors of ServeListener, of
// Terminate, of servers killed after their TermTimeout, and of closing resources. Like App, it signals each
// server's TermSig before Terminate, but it does not wait for TermSig.Wait, since no load balancer drains a
// test's traffic. Later calls return nil.
func (e *Env) Close() error {
	var err error
	e.closeOnce.Do(func() {
		errs := make([]error, len(e.served))
		var wg sync.WaitGroup
		for i, s := range e.served {
			wg.Go(func() {
				errs[i] = s.terminate()
			})
		}
		wg.Wait()
		process.WaitGoroutines()
		err = errors.Join(append(errs, e.boot.Close())...)
	})
	return err
}

// serve serves on ln until the server terminates, and keeps the error of ServeListener for Close.
func (s *servedServer) serve(ln net.Listener) {
	defer close(s.served)
	if err := s.ServeListener(ln); err != nil {
		logFailure(s.lg, err, operation.SeverityCritical, "serve error")
		s.err = fmt.Errorf("forgotest: server %q: serve: %w", s.Name(), err)
	}
}

// terminate signals the server's TermSig, terminates the server, and kills it when Terminate failed, or
// Terminate and ServeListener did not both return within TermTimeout.
func (s *servedServer) terminate() error {
	term := s.Term()
	if ts := term.TermSig; ts != nil {
		ts.Signal()
	}
	timeout := term.TermTimeout
	if timeout <= 0 {
		timeout = defaultTermTimeout
	}

	// the context of Terminate carries the server's logger
	ctx, cancel := context.WithTimeout(logger.NewCtx(context.Background(), s.lg), timeout)
	defer cancel()

	stopped := make(chan error, 1)
	go func() {
		err := s.Terminate(ctx)
		<-s.served
		stopped <- err
	}()

	select {
	case err := <-stopped:
		if err != nil {
			logFailure(s.lg, err, operation.SeverityModerate, "server termination error, killing")
			s.Kill()
			err = fmt.Errorf("forgotest: server %q: terminate: %w", s.Name(), err)
		}
		// ServeListener returned, so its error is set.
		return errors.Join(s.err, err)
	case <-ctx.Done():
		err := fmt.Errorf("forgotest: server %q: terminate: %w", s.Name(), ctx.Err())
		logFailure(s.lg, err, operation.SeverityModerate, "server termination timeout, killing")
		s.Kill()
		return err
	}
}

// logFailure logs a failure outside any operation.
func logFailure(lg zerolog.Logger, err error, severity operation.ErrorSeverity, msg string) {
	lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(severity)).Msg(msg)
}

// isNil reports whether v is nil, or holds a nil pointer, map, slice, channel or function.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	switch rv := reflect.ValueOf(v); rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
