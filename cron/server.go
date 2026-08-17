package cron

import (
	"context"
	"forgo"
	"forgo/logger"
	"forgo/middleware"
	"forgo/operation"

	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog"
)

const ServerNameCron = "cron"

// Server is a server that runs cron jobs based on their defined schedules. It uses the robfig/cron package to manage job scheduling and execution.
type Server struct {
	*forgo.Server
	jobs []IJob
	c    *cron.Cron
}

func NewServer(opts ...ServerOption) *Server {
	c := &Server{Server: &forgo.Server{}, c: cron.New()}
	for _, opt := range opts {
		opt(c)
	}
	if c.jobs == nil {
		c.jobs = make([]IJob, 0)
	}
	if c.Server.Tt == 0 {
		c.Server.Tt = forgo.DefaultServerTimeout
	}

	return c
}

func (c *Server) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	for _, job := range c.jobs {
		_, err := c.c.AddFunc(job.Schedule(), c.createHandler(ctx, job))
		if err != nil {
			return err
		}
		lg.Info().
			Str(logger.CronJobLogKey, job.Name()).
			Str(logger.CronScheduleLogKey, job.Schedule()).
			Msg("cron job added")
	}
	return nil
}

func (c *Server) Name() string {
	return ServerNameCron
}

func (c *Server) Serve() error {
	c.c.Run()
	return nil
}

func (c *Server) Terminate(context.Context) error {
	ctx := c.c.Stop()
	<-ctx.Done()
	return nil
}

func (c *Server) Kill() {}

func (c *Server) createHandler(ctx context.Context, job IJob) func() {
	var middlewares []operation.Middleware
	if !c.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(func(z zerolog.Context) zerolog.Context {
				return z
			}),
			middleware.Locker(c.Locker, job.Lock()),
			middleware.Recorder(func(event *zerolog.Event) *zerolog.Event {
				return event
			}, func(event *zerolog.Event) *zerolog.Event {
				return event
			}),
			middleware.Tolerance(job.Tolerance()),
			middleware.Recovery(),
		)
	}
	handler := func(ctx *operation.Context) error {
		return job.Run(ctx.Context)
	}
	handler = operation.Chain(handler, append(middlewares, c.Middlewares...)...)
	return func() {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    job.Name(),
			Type:    operationTypeCronJob,
		}
		handler(opCtx)
	}
}

type ServerOption func(*Server)

func WithBaseOption(opt forgo.ServerOption) ServerOption {
	return func(s *Server) {
		opt(s.Server)
	}
}

func WithJobs(jobs ...IJob) ServerOption {
	return func(server *Server) {
		server.jobs = append(server.jobs, jobs...)
	}
}
