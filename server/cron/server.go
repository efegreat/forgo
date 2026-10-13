package cron

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	robfig "github.com/robfig/cron/v3"
)

// ServerNameCron is the server_name of the cron server's logs.
const ServerNameCron = "cron"

// Server runs the jobs of a Config on their schedules. Config.New creates it.
type Server struct {
	cfg       Config
	scheduler *robfig.Cron
	locker    operation.ILocker

	// ctx is the context of every run. Only Kill cancels it.
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	terminated bool          // keeps Serve from starting the scheduler after Terminate
	done       chan struct{} // closed by Terminate, to make Serve return
}

// Term returns the termination settings of the Config.
func (s *Server) Term() forgo.TermConfig {
	return s.cfg.Term
}

// Name returns ServerNameCron.
func (s *Server) Name() string {
	return ServerNameCron
}

// Init schedules every job, with its chain built once.
func (s *Server) Init(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(context.WithoutCancel(ctx))
	lg := logger.FromCtx(ctx)
	for _, job := range s.cfg.jobs {
		s.scheduler.Schedule(job.Schedule, robfig.FuncJob(s.createRun(job)))
		lg.Info().
			Str(logger.CronJobLogKey, job.Name).
			Str(logger.CronScheduleLogKey, job.Spec).
			Msg("cron job added")
	}
	return nil
}

// Serve starts the scheduler and blocks until Terminate. After Terminate, it returns at once without
// starting it.
func (s *Server) Serve() error {
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
func (s *Server) Terminate(ctx context.Context) error {
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

// Kill cancels the context of the running jobs, which ends their Tolerance retries and every job that
// honors its context. App calls it when Terminate failed or timed out. It does not wait for the jobs.
func (s *Server) Kill() {
	if s.cancel != nil {
		s.cancel()
	}
}

// createRun builds the job's chain and returns the function the scheduler calls for each run.
func (s *Server) createRun(job resolvedJob) func() {
	var middlewares []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(s.locker, func(*operation.Context) operation.Lock { return job.Lock }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Tolerance(job.Tolerance),
			middleware.Recovery(nil),
		)
	}
	middlewares = append(middlewares, s.cfg.ResolvedMiddlewares()...)
	middlewares = append(middlewares, job.Middlewares...)
	handler := operation.Chain(func(ctx *operation.Context) error {
		return job.Job.Run(ctx)
	}, middlewares...)

	lg := logger.FromCtx(s.ctx).With().Str(logger.CronJobLogKey, job.Name).Logger()
	var running atomic.Bool
	return func() {
		// robfig starts a run whether or not the previous one returned.
		if !running.CompareAndSwap(false, true) {
			lg.Info().Msg("cron job still running, skipping")
			return
		}
		defer running.Store(false)
		// A panic outside Recovery would crash the process, since robfig recovers nothing.
		defer func() {
			if r := recover(); r != nil {
				lg.Error().
					Err(&middleware.PanicError{Value: r, Stack: debug.Stack()}).
					Int(logger.ErrorSeverityLogKey, int(operation.SeverityCritical)).
					Msg("cron job panicked")
			}
		}()

		handler(&operation.Context{
			Context: s.ctx,
			ID:      "",
			Name:    job.Name,
			Type:    OperationTypeCronJob,
			Args:    new(types.Map[any, any]),
			State:   nil, // cron jobs do not have states.
		})
	}
}
