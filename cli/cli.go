// Package cli runs one command per process invocation, with the same resources, providers and
// middleware model as forgo.App. The first argument names the command, the rest are its flags,
// and the command runs as an operation of type OperationTypeCommand.
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

	"github.com/rs/zerolog"
)

const defaultCLIName = "cli"

var (
	// ErrNoCommand is returned by Run when the command line names no command.
	ErrNoCommand = errors.New("cli: no command specified")
	// ErrUnknownCommand is returned by Run when the command line names a command that is not configured.
	ErrUnknownCommand = errors.New("cli: unknown command")

	// errLocked fails a command whose lock another run holds, so the caller can tell that it did not run.
	errLocked = operation.Error{
		Code:     "command_locked",
		Status:   operation.StatusAborted,
		Message:  "command is already running",
		Severity: operation.SeverityLight,
	}
)

// CLI runs one configured command per invocation. Create it with New and call Run from main.
type CLI struct {
	name      string
	cfg       Config
	logger    logger.Config
	args      []string
	resources []forgo.ResourceFactory
	providers []any
	locker    operation.ILocker
}

// New returns a CLI configured by opts. The name defaults to "cli".
func New(opts ...Option) *CLI {
	cli := &CLI{}
	for _, opt := range opts {
		opt(cli)
	}
	if cli.name == "" {
		cli.name = defaultCLIName
	}
	return cli
}

// Run runs the command the command line names: it bootstraps resources and providers, runs the
// command as an operation, waits for goroutines started with process.Go and closes the resources.
//
// Run never exits, so main chooses the exit code after cleanup. It returns the command's error,
// joined with any error from closing resources. Invalid configuration and lookup errors are
// returned before anything is bootstrapped, and lookup errors wrap ErrNoCommand or
// ErrUnknownCommand. The command's context is cancelled on SIGINT or SIGTERM, and a second signal
// terminates the process.
func (c *CLI) Run() (err error) {
	// create logger
	lg := c.logger.New().
		With().
		Str(logger.AppNameLogKey, c.name).Logger()

	// find the command
	if err := c.cfg.validate(); err != nil {
		logError(lg, err, operation.SeverityCritical, "invalid configuration")
		return err
	}
	args := c.args
	if args == nil {
		args = os.Args
	}
	cmd, err := c.cfg.lookup(args)
	if err != nil {
		logError(lg, err, operation.SeverityLight, "command lookup failed")
		return err
	}

	// create bootstrap
	boot := forgo.NewBootstrap(c.resources, c.providers)
	r, err := boot.Build()
	defer func() {
		if closeErr := boot.Close(); closeErr != nil {
			logError(lg, closeErr, operation.SeverityModerate, "closing resources failed")
			err = errors.Join(err, closeErr)
		}
	}()

	if err != nil {
		logError(lg, err, operation.SeverityCritical, "bootstrap failed")
		return err
	}

	// resolve command
	resolved, err := c.resolve(cmd, r)
	if err != nil {
		logError(lg, err, operation.SeverityCritical, "command resolution failed")
		return err
	}

	// seal dependencies
	if err := boot.Seal(); err != nil {
		logError(lg, err, operation.SeverityCritical, "sealing dependencies failed")
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

// logError logs a failure of Run outside the command's operation, at the level its severity implies.
func logError(lg zerolog.Logger, err error, severity operation.ErrorSeverity, msg string) {
	event := lg.Error()
	if severity == operation.SeverityLight {
		event = lg.Warn()
	}
	event.Err(err).Int(logger.ErrorSeverityLogKey, int(severity)).Msg(msg)
}

func (c *CLI) resolve(cmd CommandConfig, r forgo.Resolver) (resolvedCommand, error) {
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

func (c *CLI) createHandler(ctx context.Context, cmd resolvedCommand) func([]string) error {
	var middlewares []operation.Middleware
	if !c.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(c.locker, func(*operation.Context) operation.Lock { return cmd.Lock }, func(*operation.Context) error { return errLocked }),
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
			if errors.Is(err, flag.ErrHelp) {
				// The flag set has printed the usage, which is what was asked for.
				return nil
			}
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

// Option configures a CLI.
type Option func(*CLI)

// WithName sets the app_name of every log entry. It defaults to "cli".
func WithName(name string) Option {
	return func(c *CLI) {
		c.name = name
	}
}

// WithConfig sets the commands and middlewares. A later WithConfig replaces an earlier one.
func WithConfig(cfg Config) Option {
	return func(a *CLI) {
		a.cfg = cfg
	}
}

// WithLogger sets the logger configuration.
func WithLogger(logger logger.Config) Option {
	return func(a *CLI) {
		a.logger = logger
	}
}

// WithArgs sets the command line Run reads instead of os.Args: the program name, the command
// name, then the command's flags. Tests and programs that embed a CLI use it to choose the command.
func WithArgs(args []string) Option {
	return func(a *CLI) {
		a.args = args
	}
}

// WithResources adds resource factories. Run creates them before the providers and closes them
// in reverse order before it returns.
func WithResources(rf ...forgo.ResourceFactory) Option {
	return func(a *CLI) {
		a.resources = append(a.resources, rf...)
	}
}

// WithProviders adds provider constructors. Run invokes them in declaration order, so each may
// depend only on resources and earlier providers.
func WithProviders(p ...any) Option {
	return func(a *CLI) {
		a.providers = append(a.providers, p...)
	}
}
