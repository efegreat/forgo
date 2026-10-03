package cli

import (
	"context"
	"flag"
	"fmt"
	"forgo"
	"forgo/logger"
	"forgo/middleware"
	"forgo/operation"
	"forgo/process"
	"forgo/types"
	"os"
)

const defaultCliName = "cli"

type Cli struct {
	name      string
	cfg       Config
	logger    logger.Config
	resources []forgo.ResourceFactory
	providers []any
	commands  map[string]CommandConfig
	locker    operation.ILocker
}

func New(opts ...Option) *Cli {
	cli := &Cli{commands: make(map[string]CommandConfig)}
	for _, opt := range opts {
		opt(cli)
	}
	for _, cmd := range cli.cfg.Commands {
		cli.commands[cmd.Name] = cmd
	}
	if cli.name == "" {
		cli.name = defaultCliName
	}
	return cli
}

func (c *Cli) Run() {
	// create logger
	lg := c.logger.New().
		With().
		Str(logger.AppNameLogKey, c.name).Logger()

	// validate command
	cmd, args, err := c.validate()
	if err != nil {
		lg.Error().Err(err).Msg("validation failed")
		return
	}

	// create bootstrap
	boot := forgo.NewBootstrap(c.resources, c.providers)
	r, err := boot.Build()
	if err != nil {
		panic(err)
	}
	defer boot.Close()

	// resolve command
	resolved, err := c.resolve(cmd, r)
	if err != nil {
		panic(err)
	}

	// seal dependencies
	if err := boot.Seal(); err != nil {
		panic(err)
	}

	// create handler
	loggerCtx := logger.NewCtx(context.Background(), lg)
	handler := c.createHandler(loggerCtx, resolved)

	// execute command
	handler(args)
	process.WaitGoroutines()
}

func (c *Cli) resolve(cmd CommandConfig, r forgo.Resolver) (resolvedCommand, error) {
	locker, err := r.Resolve[operation.ILocker]()
	if err != nil {
		return resolvedCommand{}, err
	}
	c.locker = locker

	middlewares, err := forgo.ResolveMiddlewares(r, c.cfg.Middlewares)
	if err != nil {
		return resolvedCommand{}, err
	}
	cmdMiddlewares, err := forgo.ResolveMiddlewares(r, cmd.Middlewares)
	if err != nil {
		return resolvedCommand{}, err
	}
	cmdFactory, err := cmd.Command(r)
	if err != nil {
		return resolvedCommand{}, err
	}

	return resolvedCommand{
		Name:        cmd.Name,
		Lock:        cmd.Lock,
		Middlewares: append(middlewares, cmdMiddlewares...),
		Command:     cmdFactory,
	}, nil
}

func (c *Cli) validate() (CommandConfig, []string, error) {
	args := os.Args
	if len(args) < 2 {
		return CommandConfig{}, args, fmt.Errorf("no command specified")
	}

	if cmd, ok := c.commands[args[1]]; ok {
		return cmd, args, nil
	}

	var available string
	for k := range c.commands {
		available += k + ","
	}

	if len(available) < 1 {
		return CommandConfig{}, args, fmt.Errorf("command not found: %s, no commands available", args[1])
	}

	available = available[:len(available)-1]
	return CommandConfig{}, args, fmt.Errorf("command not found: %s, available: %s", args[1], available)
}

func (c *Cli) createHandler(ctx context.Context, cmd resolvedCommand) func([]string) {
	var middlewares []operation.Middleware
	if !c.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(c.locker, func(*operation.Context) operation.Lock { return cmd.Lock }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Recovery(nil),
		)
	}
	handler := func(ctx *operation.Context) error {
		exec := cmd.Command()
		fs := flag.NewFlagSet(cmd.Name, flag.ContinueOnError)
		exec.Init(fs)

		fields := ctx.State.(*State).Fields
		if err := fs.Parse(fields[2:]); err != nil {
			return operation.Error{Code: "command_parse"}.Wrap(err)
		}
		if err := exec.Validate(); err != nil {
			return operation.Error{Code: "command_validate"}.Wrap(err)
		}

		return exec.Run(ctx)
	}
	handler = operation.Chain(handler, append(middlewares, cmd.Middlewares...)...)
	return func(fields []string) {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    cmd.Name,
			Type:    OperationTypeCommand,
			Args:    types.NewMap[any, any](),
			State:   &State{Fields: fields},
		}
		handler(opCtx)
	}
}

type Option func(*Cli)

func WithName(name string) Option {
	return func(c *Cli) {
		c.name = name
	}
}

func WithConfig(cfg Config) Option {
	return func(a *Cli) {
		a.cfg = cfg
	}
}

func WithLogger(logger logger.Config) Option {
	return func(a *Cli) {
		a.logger = logger
	}
}

func WithResources(rf ...forgo.ResourceFactory) Option {
	return func(a *Cli) {
		a.resources = append(a.resources, rf...)
	}
}

func WithProviders(p ...any) Option {
	return func(a *Cli) {
		a.providers = append(a.providers, p...)
	}
}
