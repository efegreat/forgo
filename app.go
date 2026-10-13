package forgo

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/process"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

const defaultAppName = "default"

// errNoServers is returned by Run for an app without servers, which would only wait for a signal.
var errNoServers = errors.New("app: no servers")

// App runs servers that share one bootstrap, from start to graceful termination. Create it with NewApp
// and call Run from main.
type App struct {
	name      string
	logger    logger.Config
	resources []ResourceFactory
	providers []any
	servers   []ServerFactory
}

// NewApp returns an App configured by opts. The name defaults to "default".
func NewApp(opts ...AppOption) *App {
	app := &App{}
	for _, opt := range opts {
		opt(app)
	}
	if app.name == "" {
		app.name = defaultAppName
	}
	return app
}

// Run builds the bootstrap, creates and initializes every server, seals the bootstrap and serves until
// SIGINT or SIGTERM, or until a server's Serve fails. It then terminates every server in parallel, waits
// for the goroutines started with process.Go and closes the resources. A signal during termination
// terminates the process, such as a second Ctrl+C.
//
// Run never exits, so main chooses the exit code after cleanup:
//
//	if err := app.Run(); err != nil {
//		os.Exit(1)
//	}
//
// Run logs every failure, and returns nil only when a signal stopped the app and every server and resource
// shut down cleanly. Otherwise it returns the failures joined: the one that kept the app from starting, or
// the Serve error that stopped it, with the servers that failed to terminate in time and the resources
// that failed to close.
func (a *App) Run() (err error) {
	// create logger, also the default of contexts without one, such as those of resources and providers
	lg := a.logger.New().
		With().
		Str(logger.AppNameLogKey, a.name).
		Str(logger.AppIDLogKey, xid.New().String()).
		Logger()
	defer logger.SetDefault(lg)()

	if err := a.validate(); err != nil {
		logFailure(lg, err, operation.SeverityCritical, "invalid configuration")
		return err
	}

	// create bootstrap
	boot := NewBootstrap(a.resources, a.providers)
	resolver, err := boot.Build()
	if err != nil {
		logFailure(lg, err, operation.SeverityCritical, "bootstrap failed")
		return err
	}
	defer func() {
		if closeErr := boot.Close(); closeErr != nil {
			logFailure(lg, closeErr, operation.SeverityModerate, "closing resources failed")
			err = errors.Join(err, closeErr)
		}
	}()

	// create and init servers
	servers, err := a.initServers(lg, resolver)
	if err != nil {
		logFailure(lg, err, operation.SeverityCritical, "server start failed")
		return err
	}

	// seal dependencies
	if err := boot.Seal(); err != nil {
		logFailure(lg, err, operation.SeverityCritical, "sealing dependencies failed")
		return err
	}

	// catch SIGINT and SIGTERM until termination starts
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// start servers
	failed := make(chan error, len(servers))
	for _, s := range servers {
		go s.serve(failed)
	}

	// block until a signal or a serve error
	var serveErr error
	select {
	case <-ctx.Done():
		lg.Info().Msgf("app terminating: %v", context.Cause(ctx))
	case serveErr = <-failed:
		lg.Info().Msg("app terminating after a server failed")
	}
	// from now on, a signal falls back to the default behavior and terminates the process
	stop()

	// terminate servers in parallel
	errs := make([]error, 1+len(servers))
	errs[0] = serveErr
	var wg sync.WaitGroup
	for i, s := range servers {
		wg.Go(func() {
			errs[1+i] = s.terminate()
		})
	}
	wg.Wait()
	process.WaitGoroutines()

	return errors.Join(errs...)
}

// validate checks the server list before anything is bootstrapped.
func (a *App) validate() error {
	if len(a.servers) == 0 {
		return errNoServers
	}
	for i, sf := range a.servers {
		if isNilAny(sf) {
			return fmt.Errorf("app: server %d is nil", i)
		}
	}
	return nil
}

// initServers creates every server with its factory and initializes it with a context that carries its
// logger.
func (a *App) initServers(lg zerolog.Logger, resolver Resolver) ([]*appServer, error) {
	servers := make([]*appServer, len(a.servers))
	for i, sf := range a.servers {
		s, err := sf.New(resolver)
		if err != nil {
			return nil, fmt.Errorf("app: server %q: new: %w", sf.Name(), err)
		}
		if isNilAny(s) {
			return nil, fmt.Errorf("app: server %q: New returned nil", sf.Name())
		}

		sLg := lg.With().
			Str(logger.ServerIDLogKey, xid.New().String()).
			Str(logger.ServerNameLogKey, s.Name()).
			Logger()
		if err := s.Init(logger.NewCtx(context.Background(), sLg)); err != nil {
			return nil, fmt.Errorf("app: server %q: init: %w", s.Name(), err)
		}
		servers[i] = &appServer{IServer: s, lg: sLg, served: make(chan struct{})}
	}
	return servers, nil
}

// appServer is a server of a running App.
type appServer struct {
	IServer
	lg     zerolog.Logger
	served chan struct{} // closed when Serve returned
}

// serve runs Serve, and sends its error to failed.
func (s *appServer) serve(failed chan<- error) {
	defer close(s.served)
	s.lg.Info().Msg("server starting")
	if err := s.Serve(); err != nil {
		logFailure(s.lg, err, operation.SeverityCritical, "serve error")
		failed <- fmt.Errorf("app: server %q: serve: %w", s.Name(), err)
	}
}

// terminate signals the server's TermSig, waits for its Wait, then terminates the server, and kills it when
// Terminate and Serve did not both return within TermTimeout.
func (s *appServer) terminate() error {
	term := s.Term()
	if ts := term.TermSig; ts != nil {
		ts.Signal()
		s.lg.Info().Msgf("server termination signaled, waiting: %s", ts.Wait)
		time.Sleep(ts.Wait)
	}
	timeout := term.TermTimeout
	if timeout <= 0 {
		timeout = defaultServerTimeout
	}

	// the context of Terminate carries the server's logger
	ctx, cancel := context.WithTimeout(logger.NewCtx(context.Background(), s.lg), timeout)
	defer cancel()

	stopped := make(chan error, 1)
	go func() {
		s.lg.Info().Msg("server is terminating")
		err := s.Terminate(ctx)
		<-s.served
		stopped <- err
	}()

	select {
	case err := <-stopped:
		if err != nil {
			logFailure(s.lg, err, operation.SeverityModerate, "server termination error")
			return fmt.Errorf("app: server %q: terminate: %w", s.Name(), err)
		}
		s.lg.Info().Msg("server termination completed")
		return nil
	case <-ctx.Done():
		err := fmt.Errorf("app: server %q: terminate: %w", s.Name(), ctx.Err())
		logFailure(s.lg, err, operation.SeverityModerate, "server termination timeout, killing")
		s.Kill()
		return err
	}
}

// logFailure logs a failure of Run outside any operation.
func logFailure(lg zerolog.Logger, err error, severity operation.ErrorSeverity, msg string) {
	lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(severity)).Msg(msg)
}

// AppOption configures an App.
type AppOption func(*App)

// WithName sets the app_name of the app's logs.
func WithName(name string) AppOption {
	return func(a *App) {
		a.name = name
	}
}

// WithLogger sets the configuration of the app's logger.
func WithLogger(cfg logger.Config) AppOption {
	return func(a *App) {
		a.logger = cfg
	}
}

// WithResources appends resource factories. Resources are created in order and closed in reverse order.
func WithResources(rf ...ResourceFactory) AppOption {
	return func(a *App) {
		a.resources = append(a.resources, rf...)
	}
}

// WithProviders appends providers: constructors whose results the bootstrap binds, called once, in order.
// A provider may only depend on resources and on the providers before it.
func WithProviders(p ...any) AppOption {
	return func(a *App) {
		a.providers = append(a.providers, p...)
	}
}

// WithServers appends server factories. Servers are created and initialized in order, and served and
// terminated in parallel.
func WithServers(sf ...ServerFactory) AppOption {
	return func(a *App) {
		a.servers = append(a.servers, sf...)
	}
}
