package forgo

import (
	"context"
	"errors"
	"fmt"
	"forgo/logger"
	"forgo/process"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

type App struct {
	logger    logger.Config
	resources []ResourceFactory
	providers []any
	servers   []ServerFactory
}

func NewApp(opts ...AppOption) *App {
	app := &App{}
	for _, opt := range opts {
		opt(app)
	}
	return app
}

func (a *App) Run() {
	// create bootstrap
	boot := NewBootstrap(a.resources, a.providers)
	resolver, err := boot.Build()
	if err != nil {
		panic(err)
	}
	defer boot.Close()

	// create a logger context
	lg := a.logger.New().With().
		Str(logger.AppIDLogKey, xid.New().String()).
		Logger()

	// init servers
	servers := make([]IServer, len(a.servers))
	loggerByServer := make(map[int]zerolog.Logger)
	for i, sc := range a.servers {
		s, err := sc.New(resolver)
		if err != nil {
			panic(fmt.Errorf("server create error, name: %s, err: %w", sc.Name(), err))
		}

		sLg := lg.With().
			Str(logger.ServerIDLogKey, xid.New().String()).
			Str(logger.ServerNameLogKey, s.Name()).
			Logger()
		loggerByServer[i] = sLg

		loggerCtx := logger.NewCtx(context.Background(), sLg)
		if err := s.Init(loggerCtx); err != nil {
			panic(fmt.Errorf("server init error, name: %s, err: %w", s.Name(), err))
		}

		servers[i] = s
	}

	// seal dependencies
	if err := boot.Seal(); err != nil {
		panic(err)
	}

	// create a context that will be canceled when SIGINT or SIGTERM is received
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// create an error channel
	ec := make(chan error, len(servers))

	// starting servers
	for i, s := range servers {
		go func() {
			sLg := loggerByServer[i]
			sLg.Info().Msg("server starting")
			if err := s.Serve(); err != nil {
				sLg.Error().Err(err).Msg("serve error")
				ec <- err
			}
		}()
	}

	// block until a signal or error is received
	select {
	case <-ctx.Done():
		break
	case <-ec:
		break
	}

	// terminating servers
	wg := &sync.WaitGroup{}
	for i, s := range servers {
		sLg := loggerByServer[i]
		wg.Go(func() {
			// termination initiated, wait for readiness probe if set
			if ts := s.Term().TermSig; ts != nil {
				ts.Signal()
				sLg.Info().Msgf("server termination signaled, waiting: %s", ts.Wait.String())
				time.Sleep(ts.Wait)
			}

			// create a context with timeout for graceful shutdown, with a given timeout
			ctx, cancel := context.WithTimeout(context.Background(), s.Term().TermTimeout)
			defer cancel()

			// use a channel to signal when graceful shutdown is done
			done := make(chan struct{})
			go func() {
				sLg.Info().Msg("server is terminating")
				if err := s.Terminate(ctx); err != nil {
					sLg.Error().Err(err).Msg("server termination error")
				}
				close(done)
			}()

			// wait for either termination to complete or timeout
			select {
			case <-done:
				sLg.Info().Msg("server termination completed")
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					sLg.Info().Msg("server termination timeout, killing")
					s.Kill()
				}
			}
		})
	}

	wg.Wait()
	process.WaitGoroutines()
}

type AppOption func(*App)

func WithLogger(logger logger.Config) AppOption {
	return func(a *App) {
		a.logger = logger
	}
}

func WithResources(rf ...ResourceFactory) AppOption {
	return func(a *App) {
		a.resources = append(a.resources, rf...)
	}
}

func WithProviders(p ...any) AppOption {
	return func(a *App) {
		a.providers = append(a.providers, p...)
	}
}

func WithServers(sf ...ServerFactory) AppOption {
	return func(a *App) {
		a.servers = append(a.servers, sf...)
	}
}
