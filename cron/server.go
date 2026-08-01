package cron

import (
	"context"
	"forgo"
	"forgo/logger"
	"forgo/operation"

	"github.com/robfig/cron/v3"
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
		fn := operation.NewHandler(*lg, c.Locker, JobOperation{job: job})
		_, err := c.c.AddFunc(job.Schedule(), fn)
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

type ServerOption func(*Server)

func WithBaseOption(opt forgo.ServerOption) ServerOption {
	return func(s *Server) {
		opt(s.Server)
	}
}

func WithJob(job IJob) ServerOption {
	return func(server *Server) {
		server.jobs = append(server.jobs, job)
	}
}
