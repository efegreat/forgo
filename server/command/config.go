package command

import (
	"forgo"
	"forgo/operation"
	"forgo/process"
)

const defaultMaxConcurrent = 10

type Config struct {
	forgo.ServerConfig
	MaxConcurrent int
	Executions    []ExecutionConfig
	executions    []resolvedExecution
}

func (c Config) Sanitize() Config {
	c.ServerConfig = c.ServerConfig.Sanitize()
	if c.MaxConcurrent < 1 {
		c.MaxConcurrent = defaultMaxConcurrent
	}
	return c
}

func (c Config) Name() string {
	return ServerNameCommand
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
	cfg, err = cfg.resolveExecutions(r)
	if err != nil {
		return nil, err
	}

	return &Server{cfg: cfg, handlers: make(map[string]Handler), locker: locker, sem: process.NewChannelSemaphore(cfg.MaxConcurrent)}, nil
}

func (c Config) resolveExecutions(r forgo.Resolver) (Config, error) {
	for _, ex := range c.Executions {
		ef, err := ex.Exec(r)
		if err != nil {
			return Config{}, err
		}
		c.executions = append(c.executions, resolvedExecution{
			Name:       ex.Name,
			Concurrent: ex.Concurrent,
			Lock:       ex.Lock,
			NewExec:    ef,
		})
	}
	return c, nil
}

type ExecutionConfig struct {
	Name       string
	Concurrent bool
	Lock       operation.Lock
	Exec       Factory
}

type resolvedExecution struct {
	Name       string
	Concurrent bool
	Lock       operation.Lock
	NewExec    ExecutionFactory
}

type Factory func(r forgo.Resolver) (ExecutionFactory, error)

func As(
	constructor any,
) Factory {
	return func(
		r forgo.Resolver,
	) (ExecutionFactory, error) {
		return r.Call[ExecutionFactory](
			constructor,
		)
	}
}
