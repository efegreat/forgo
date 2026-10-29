package cron

import (
	"context"
	"fmt"
	"sync"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/inprocess"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/safemap"

	robfig "github.com/robfig/cron/v3"
)

// ServerNameCron is the default name of the cron server, the server_name of its logs.
const ServerNameCron = "cron"

// server runs the jobs of a Config on their schedules. Config.New creates it.
type server struct {
	cfg       Config
	scheduler *robfig.Cron
	locker    operation.ILocker
	observer  operation.IObserver

	// ctx is the context of every run. Only Kill cancels it.
	ctx    context.Context
	cancel context.CancelFunc
	// runs run the jobs once each, by name. Init builds them.
	runs map[string]func(ctx context.Context) error

	mu         sync.Mutex
	terminated bool          // keeps Serve from starting the scheduler after Terminate
	done       chan struct{} // closed by Terminate, to make Serve return
}

// Term returns the termination settings of the Config.
func (s *server) Term() forgo.TermConfig {
	return s.cfg.Term
}

// Name returns the name of the config.
func (s *server) Name() string {
	return s.cfg.Name()
}

// Init schedules every job, with its chain built once.
func (s *server) Init(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(context.WithoutCancel(ctx))
	lg := logger.FromCtx(ctx)
	s.runs = make(map[string]func(context.Context) error, len(s.cfg.jobs))
	for _, job := range s.cfg.jobs {
		run := s.createRun(job)
		s.runs[job.Name] = run
		s.scheduler.Schedule(job.Schedule, robfig.FuncJob(func() { _ = run(s.ctx) }))
		lg.Info().
			Str(logger.CronJobLogKey, job.Name).
			Str(logger.CronScheduleLogKey, job.Spec).
			Msg("cron job added")
	}
	return nil
}

// Serve starts the scheduler and blocks until Terminate. After Terminate, it returns at once without
// starting it.
func (s *server) Serve() error {
	s.mu.Lock()
	if s.terminated {
		s.mu.Unlock()
		return nil
	}
	s.scheduler.Start()
	s.mu.Unlock()

	<-s.done
	return nil
}

// Terminate stops scheduling and waits for the running jobs to finish. When ctx ends first, it returns
// ctx's error and leaves the jobs running, for App to Kill the server.
func (s *server) Terminate(ctx context.Context) error {
	s.mu.Lock()
	if !s.terminated {
		s.terminated = true
		close(s.done)
	}
	running := s.scheduler.Stop()
	s.mu.Unlock()

	select {
	case <-running.Done():
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cron: waiting for running jobs: %w", ctx.Err())
	}
}

// Jobs returns a function per job, by name, that runs the job once through its chain, as its schedule does,
// and returns the error of the chain. forgotest calls it after Init. The run ends with ctx, or when the
// server is killed, and looks a value up in ctx, then in the context of the server's runs, such as its
// logger.
func (s *server) Jobs() map[string]func(ctx context.Context) error {
	jobs := make(map[string]func(context.Context) error, len(s.runs))
	for name, run := range s.runs {
		jobs[name] = func(ctx context.Context) error {
			ctx, cancel := context.WithCancel(inprocess.Context(ctx, s.ctx))
			defer cancel()
			stop := context.AfterFunc(s.ctx, cancel)
			defer stop()
			return run(ctx)
		}
	}
	return jobs
}

// Kill cancels the context of the running jobs, which ends their Tolerance retries and every job that
// honors its context. App calls it when Terminate failed or timed out. It does not wait for the jobs.
func (s *server) Kill() {
	if s.cancel != nil {
		s.cancel()
	}
}

// createRun builds the job's chain and returns the function that runs the job once on ctx, and returns the
// error of the chain. The scheduler calls it with the server's ctx.
func (s *server) createRun(job resolvedJob) func(ctx context.Context) error {
	var middlewares []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(s.locker, func(*operation.Context) operation.Lock { return job.Lock }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Observe(s.observer),
			middleware.Tolerance(job.Tolerance),
			middleware.Recovery(nil),
		)
	}
	middlewares = append(middlewares, s.cfg.ResolvedMiddlewares()...)
	middlewares = append(middlewares, job.Middlewares...)
	handler := operation.Chain(func(ctx *operation.Context) error {
		return job.Job.Run(ctx)
	}, middlewares...)

	return func(ctx context.Context) error {
		return handler(&operation.Context{
			Context: ctx,
			ID:      "",
			Name:    job.Name,
			Type:    OperationTypeCronJob,
			Args:    new(safemap.Map[any, any]),
			State:   nil, // cron jobs do not have states.
		})
	}
}
