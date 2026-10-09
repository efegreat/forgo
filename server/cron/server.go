package cron

import (
	"context"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/robfig/cron/v3"
)

const ServerNameCron = "cron"

// Server is a server that runs cron jobs based on their defined schedules. It uses the robfig/cron package to manage job scheduling and execution.
type Server struct {
	cfg    Config
	c      *cron.Cron
	locker operation.ILocker
}

func (c *Server) Term() forgo.TermConfig {
	return c.cfg.Term
}

func (c *Server) Name() string {
	return ServerNameCron
}

func (c *Server) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	for _, job := range c.cfg.jobs {
		_, err := c.c.AddFunc(job.Schedule, c.createHandler(ctx, job))
		if err != nil {
			return err
		}
		lg.Info().
			Str(logger.CronJobLogKey, job.Name).
			Str(logger.CronScheduleLogKey, job.Schedule).
			Msg("cron job added")
	}
	return nil
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

func (c *Server) createHandler(ctx context.Context, job resolvedJob) func() {
	var middlewares []operation.Middleware
	if !c.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(c.locker, func(ctx *operation.Context) operation.Lock { return job.Lock }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Tolerance(job.Job.Tolerance()),
			middleware.Recovery(nil),
		)
	}
	handler := func(ctx *operation.Context) error {
		return job.Job.Run(ctx)
	}
	handler = operation.Chain(handler, append(middlewares, c.cfg.ResolvedMiddlewares()...)...)
	return func() {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    job.Name,
			Type:    OperationTypeCronJob,
			Args:    new(types.Map[any, any]),
			State:   nil, // cron jobs do not have states.
		}
		handler(opCtx)
	}
}
