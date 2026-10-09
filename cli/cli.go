package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/types"
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

func (c *Cli) Run() error {
	// create logger
	lg := c.logger.New().
		With().
		Str(logger.AppNameLogKey, c.name).Logger()

	// validate command
	cmd, args, err := c.validate()
	if err != nil {
		lg.Error().Err(err).Msg("validation failed")
		return err
	}

	// create bootstrap
	boot := forgo.NewBootstrap(c.resources, c.providers)
	r, err := boot.Build()
	defer boot.Close()

	if err != nil {
		lg.Error().Err(err).Msg("bootstrap failed")
		return err
	}

	// resolve command
	resolved, err := c.resolve(cmd, r)
	if err != nil {
		lg.Error().Err(err).Msg("command resolution failed")
		return err
	}

	// seal dependencies
	if err := boot.Seal(); err != nil {
		lg.Error().Err(err).Msg("sealing dependencies failed")
		return err
	}

	// cancel the command on SIGINT/SIGTERM; a second signal falls back to the default behavior
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop)

	// create handler
	loggerCtx := logger.NewCtx(ctx, lg)
	handler := c.createHandler(loggerCtx, resolved)

	// execute command
	err = handler(args)
	process.WaitGoroutines()

	return err
}

func (c *Cli) resolve(cmd CommandConfig, r forgo.Resolver) (resolvedCommand, error) {
	locker, err := r.Resolve[operation.ILocker]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return resolvedCommand{}, err
	}
	if locker == nil && cmd.Lock.Enabled() && !c.cfg.DisableBaseMiddlewares {
		return resolvedCommand{}, fmt.Errorf("cli: command %q has a lock but no operation.ILocker is provided", cmd.Name)
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

func (c *Cli) createHandler(ctx context.Context, cmd resolvedCommand) func([]string) error {
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
			return operation.Error{
				Code:    "command_parse",
				Status:  operation.StatusInvalidArgument,
				Message: "invalid arguments: " + err.Error(),
			}.Wrap(err)
		}
		if err := exec.Validate(); err != nil {
			return operation.Error{
				Code:    "command_validate",
				Status:  operation.StatusInvalidArgument,
				Message: "invalid command: " + err.Error(),
			}.Wrap(err)
		}

		return exec.Run(ctx)
	}
	handler = operation.Chain(handler, append(middlewares, cmd.Middlewares...)...)
	return func(fields []string) error {
		opCtx := &operation.Context{
			Context: ctx,
			ID:      "",
			Name:    cmd.Name,
			Type:    OperationTypeCommand,
			Args:    new(types.Map[any, any]),
			State:   &State{Fields: fields},
		}
		return handler(opCtx)
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
