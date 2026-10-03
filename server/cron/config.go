package cron

import (
	"forgo"
	"forgo/operation"

	"github.com/robfig/cron/v3"
)

type Config struct {
	forgo.ServerConfig
	Jobs []JobConfig
	jobs []resolvedJob
}

func (c Config) Sanitize() Config {
	c.ServerConfig = c.ServerConfig.Sanitize()
	return c
}

func (c Config) Name() string {
	return ServerNameCron
}

func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	locker, err := r.Resolve[operation.ILocker]()
	if err != nil {
		return nil, err
	}

	cfg := c.Sanitize()
	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc
	cfg, err = cfg.resolveJobs(r)
	if err != nil {
		return nil, err
	}

	return &Server{cfg: cfg, c: cron.New(), locker: locker}, nil
}

func (c Config) resolveJobs(r forgo.Resolver) (Config, error) {
	for _, job := range c.Jobs {
		rj, err := job.Job(r)
		if err != nil {
			return Config{}, err
		}
		c.jobs = append(c.jobs, resolvedJob{
			Name:     job.Name,
			Schedule: job.Schedule,
			Lock:     job.Lock,
			Job:      rj,
		})
	}
	return c, nil
}

type JobConfig struct {
	Name     string
	Schedule string
	Lock     operation.Lock
	Job      JobFactory
}

type resolvedJob struct {
	Name     string
	Schedule string
	Lock     operation.Lock
	Job      IJob
}

type JobFactory func(r forgo.Resolver) (IJob, error)

func As(
	constructor any,
) JobFactory {
	return func(
		r forgo.Resolver,
	) (IJob, error) {
		return r.Call[IJob](
			constructor,
		)
	}
}
