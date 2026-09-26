package cron

import (
	"context"
	"forgo"
	"forgo/logger"
	"forgo/middleware"
	"forgo/operation"
	"forgo/types"

	"github.com/robfig/cron/v3"
)

const ServerNameCron = "cron"

type Config struct {
	forgo.ServerConfig
	Jobs []IJob
}

func (c Config) Sanitize() Config {
	if c.TermTimeout == 0 {
		c.TermTimeout = forgo.DefaultServerTimeout
	}
	return c
}

// Server is a server that runs cron jobs based on their defined schedules. It uses the robfig/cron package to manage job scheduling and execution.
type Server struct {
	cfg Config
	c   *cron.Cron
}

func NewServer(cfg Config) *Server {
	return &Server{cfg: cfg.Sanitize(), c: cron.New()}
}

func (c *Server) Config() forgo.ServerConfig {
	return c.cfg.ServerConfig
}

func (c *Server) Name() string {
	return ServerNameCron
}

func (c *Server) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	for _, job := range c.cfg.Jobs {
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
	if !c.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(c.cfg.Locker, func(ctx *operation.Context) operation.Lock { return job.Lock() }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Tolerance(job.Tolerance()),
			middleware.Recovery(nil),
		)
	}
	handler := func(ctx *operation.Context) error {
		return job.Run(ctx)
	}
	handler = operation.Chain(handler, append(middlewares, c.cfg.Middlewares...)...)
	return func() {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    job.Name(),
			Type:    OperationTypeCronJob,
			Args:    types.NewMap(),
			State:   nil, // cron jobs do not have states.
		}
		handler(opCtx)
	}
}
