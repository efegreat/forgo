package cron

import (
	"errors"
	"fmt"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"

	robfig "github.com/robfig/cron/v3"
)

// Config declares a cron server and its jobs. It is a forgo.ServerFactory, so it can be declared as a
// package variable and passed to forgo.WithServers.
type Config struct {
	forgo.ServerConfig
	// Jobs are the jobs the server runs. Names must be unique and non-empty.
	Jobs []JobConfig

	jobs []resolvedJob
}

// Sanitize returns a copy of c with the defaults of forgo.ServerConfig applied.
func (c Config) Sanitize() Config {
	c.ServerConfig = c.ServerConfig.Sanitize()
	return c
}

// Name returns ServerNameCron.
func (c Config) Name() string {
	return ServerNameCron
}

// New validates the jobs, parses their schedules and resolves their jobs and middlewares, so that a
// configuration error fails at startup with the name of its job.
func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	cfg := c.Sanitize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	locker, err := r.Resolve[operation.ILocker]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}
	if locker == nil && !cfg.DisableBaseMiddlewares {
		for _, job := range cfg.Jobs {
			if job.Lock.Enabled() {
				return nil, fmt.Errorf("cron: job %q has a lock but no operation.ILocker is provided", job.Name)
			}
		}
	}

	observer, err := r.Resolve[operation.IObserver]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}

	sc, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	cfg.ServerConfig = sc
	cfg, err = cfg.resolveJobs(r)
	if err != nil {
		return nil, err
	}

	return &server{
		cfg:       cfg,
		scheduler: robfig.New(),
		locker:    operation.ObserveLocks(locker, observer),
		observer:  observer,
		done:      make(chan struct{}),
	}, nil
}

// validate rejects the jobs that cannot run or cannot be told apart in logs.
func (c Config) validate() error {
	seen := make(map[string]struct{}, len(c.Jobs))
	for i, job := range c.Jobs {
		if job.Name == "" {
			return fmt.Errorf("cron: job %d has no name", i)
		}
		if job.Job == nil {
			return fmt.Errorf("cron: job %q has no Job", job.Name)
		}
		if _, ok := seen[job.Name]; ok {
			return fmt.Errorf("cron: duplicate job %q", job.Name)
		}
		seen[job.Name] = struct{}{}
	}
	return nil
}

func (c Config) resolveJobs(r forgo.Resolver) (Config, error) {
	for _, job := range c.Jobs {
		schedule, err := robfig.ParseStandard(job.Schedule)
		if err != nil {
			return Config{}, fmt.Errorf("cron: job %q: schedule %q: %w", job.Name, job.Schedule, err)
		}
		middlewares, err := forgo.ResolveMiddlewares(r, job.Middlewares)
		if err != nil {
			return Config{}, fmt.Errorf("cron: job %q: %w", job.Name, err)
		}
		rj, err := job.Job(r)
		if err != nil {
			return Config{}, fmt.Errorf("cron: job %q: %w", job.Name, err)
		}
		c.jobs = append(c.jobs, resolvedJob{
			Name:        job.Name,
			Spec:        job.Schedule,
			Schedule:    schedule,
			Lock:        job.Lock,
			Tolerance:   rj.Tolerance(),
			Middlewares: middlewares,
			Job:         rj,
		})
	}
	return c, nil
}

// JobConfig declares one job.
type JobConfig struct {
	// Name is the operation name of the job's runs, op_name in logs.
	Name string
	// Schedule is a standard 5-field spec (minute, hour, day of month, month, day of week), a
	// descriptor such as @daily, or @every <duration>. A CRON_TZ=<zone> prefix sets its time zone,
	// which is the process's local time otherwise.
	Schedule string
	// Lock keeps two runs of the job from running at once, in this process or another, when both Key
	// and TTL are set. It needs an operation.ILocker in the bootstrap. A run whose lock is held is
	// skipped. Its TTL must cover the whole run, retries included. The base chain applies it, so
	// DisableBaseMiddlewares ignores it.
	Lock operation.Lock
	// Middlewares wrap this job, inside the server's middlewares.
	Middlewares []forgo.MiddlewareFactory
	// Job creates the job, usually with As.
	Job JobFactory
}

type resolvedJob struct {
	Name        string
	Spec        string
	Schedule    robfig.Schedule
	Lock        operation.Lock
	Tolerance   operation.Tolerance
	Middlewares []operation.Middleware
	Job         IJob
}

// JobFactory resolves a job's dependencies and returns the job. Config.New calls it once per job.
type JobFactory func(r forgo.Resolver) (IJob, error)

// As returns a JobFactory that calls constructor with its parameters resolved by the bootstrap. The
// constructor returns an IJob, optionally with an error.
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
