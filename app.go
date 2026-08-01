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
	logger  zerolog.Logger
	servers []IServer
}

func NewApp(opts ...AppOption) *App {
	app := &App{}
	for _, opt := range opts {
		opt(app)
	}
	return app
}

func (a *App) Run() {
	// create a context that will be canceled when SIGINT or SIGTERM is received
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// create an error channel
	ec := make(chan error, len(a.servers))

	// create a logger context
	lg := a.logger.With().
		Str(logger.AppIDLogKey, xid.New().String()).
		Logger()
	loggerByServer := make(map[int]zerolog.Logger)

	// starting servers
	for i, s := range a.servers {
		lg = lg.With().
			Str(logger.ServerIDLogKey, xid.New().String()).
			Str(logger.ServerNameLogKey, s.Name()).
			Logger()
		loggerCtx := logger.NewCtx(context.Background(), lg)
		loggerByServer[i] = lg
		go func() {
			if err := s.Init(loggerCtx); err != nil {
				ec <- fmt.Errorf("server init error, name: %s, err: %w", s.Name(), err)
				return
			}
			lg.Info().Msg("server starting")
			if err := s.Serve(); err != nil {
				ec <- fmt.Errorf("serve error, name: %s, err: %w", s.Name(), err)
			}
		}()
	}

	// block until a signal or error is received
	select {
	case <-ctx.Done():
		break
	case err := <-ec:
		panic(err)
	}

	// terminating servers
	wg := &sync.WaitGroup{}
	for i, s := range a.servers {
		lg := loggerByServer[i]
		wg.Go(func() {
			// termination initiated, wait for readiness probe if set
			if ts := s.TermSig(); ts != nil {
				ts.Signal()
				lg.Info().Msgf("server termination signaled, waiting: %s", ts.Wait.String())
				time.Sleep(ts.Wait)
			}

			// create a context with timeout for graceful shutdown, with a given timeout
			ctx, cancel := context.WithTimeout(context.Background(), s.TermTimeout())
			defer cancel()

			// use a channel to signal when graceful shutdown is done
			done := make(chan struct{})
			go func() {
				lg.Info().Msg("server is terminating")
				if err := s.Terminate(ctx); err != nil {
					lg.Error().Err(err).Msg("server termination error")
				}
				close(done)
			}()

			// wait for either termination to complete or timeout
			select {
			case <-done:
				lg.Info().Msg("server termination completed")
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					lg.Info().Msg("server termination timeout, killing")
					s.Kill()
				}
			}
		})
	}

	wg.Wait()
	process.WaitGoroutines()
}

type AppOption func(*App)

func WithLogger(logger zerolog.Logger) AppOption {
	return func(a *App) {
		a.logger = logger
	}
}

func WithServer(srv IServer) AppOption {
	return func(a *App) {
		a.servers = append(a.servers, srv)
	}
}
