package forgo

import (
	"context"
	"forgo/logger"
	"forgo/operation"

	"github.com/robfig/cron/v3"
)

type ICronJob interface {
	Name() string
	Tolerance() operation.Tolerance
	Lock() operation.Lock
	Schedule() string
	Run(ctx context.Context) operation.Error
}

type CronJobOperation struct {
	job ICronJob
}

func (c CronJobOperation) ID() string {
	return ""
}

func (c CronJobOperation) Name() string {
	return c.job.Name()
}

func (c CronJobOperation) Type() operation.Type {
	return operation.TypeCronJob
}

func (c CronJobOperation) Tolerance() operation.Tolerance {
	return c.job.Tolerance()
}

func (c CronJobOperation) Lock() operation.Lock {
	return c.job.Lock()
}

func (c CronJobOperation) BeforeRun(context.Context) operation.Error {
	return operation.Error{}
}

func (c CronJobOperation) Run(ctx context.Context) operation.Error {
	return c.job.Run(ctx)
}

// CronServer is a server that runs cron jobs based on their defined schedules. It uses the robfig/cron package to manage job scheduling and execution.
type CronServer struct {
	*Server
	c    *cron.Cron
	jobs []ICronJob
}

func NewCronServer(jobs []ICronJob, opts ...ServerOption) *CronServer {
	return &CronServer{Server: NewServer(opts...), c: cron.New(), jobs: jobs}
}

func (c *CronServer) Init(ctx context.Context) error {
	lg := logger.FromCtx(ctx)
	for _, job := range c.jobs {
		fn := operation.NewHandler(*lg, c.locker, CronJobOperation{job: job})
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

func (c *CronServer) Name() ServerName {
	return ServerNameCron
}

func (c *CronServer) Serve() error {
	c.c.Run()
	return nil
}

func (c *CronServer) Terminate(context.Context) error {
	ctx := c.c.Stop()
	<-ctx.Done()
	return ctx.Err()
}

func (c *CronServer) Kill() {}
