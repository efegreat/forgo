package cli

import (
	"errors"
	"fmt"
	"strings"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

// Config declares the commands a CLI can run and the middlewares around them.
type Config struct {
	// DisableBaseMiddlewares drops the built-in chain: ID, Logger, Locker, Recorder and Recovery.
	// Command locks are applied by Locker, so they are ignored too.
	DisableBaseMiddlewares bool
	// Middlewares wrap every command, inside the built-in chain.
	Middlewares []forgo.MiddlewareFactory
	// Commands are the commands the CLI can run. Names must be unique and non-empty.
	Commands []CommandConfig
}

// CommandConfig declares one command.
type CommandConfig struct {
	// Name is the first argument, which selects the command.
	Name string
	// Lock keeps the command from running concurrently across processes when both Key and TTL
	// are set. It needs an operation.ILocker in the bootstrap. While another run holds the lock,
	// Run returns an error with code command_locked without running the command.
	Lock operation.Lock
	// Middlewares wrap this command, inside the CLI's middlewares.
	Middlewares []forgo.MiddlewareFactory
	// Command creates the command's factory, usually with As.
	Command Factory
}

// validate rejects configurations whose commands cannot be told apart or created.
func (c Config) validate() error {
	if len(c.Commands) == 0 {
		return errors.New("cli: no commands configured")
	}
	seen := make(map[string]struct{}, len(c.Commands))
	for i, cmd := range c.Commands {
		if cmd.Name == "" {
			return fmt.Errorf("cli: command %d has no name", i)
		}
		if cmd.Command == nil {
			return fmt.Errorf("cli: command %q has no Command", cmd.Name)
		}
		if _, ok := seen[cmd.Name]; ok {
			return fmt.Errorf("cli: duplicate command %q", cmd.Name)
		}
		seen[cmd.Name] = struct{}{}
	}
	return nil
}

// lookup returns the command args[1] names. Its errors list the available commands in
// declaration order.
func (c Config) lookup(args []string) (CommandConfig, error) {
	if len(args) >= 2 {
		for _, cmd := range c.Commands {
			if cmd.Name == args[1] {
				return cmd, nil
			}
		}
	}

	names := make([]string, len(c.Commands))
	for i, cmd := range c.Commands {
		names[i] = cmd.Name
	}
	available := strings.Join(names, ", ")
	if len(args) < 2 {
		return CommandConfig{}, fmt.Errorf("%w, available: %s", ErrNoCommand, available)
	}
	return CommandConfig{}, fmt.Errorf("%w %q, available: %s", ErrUnknownCommand, args[1], available)
}

type resolvedCommand struct {
	Name        string
	Lock        operation.Lock
	Middlewares []operation.Middleware
	Command     CommandFactory
}

// Factory resolves a command's dependencies and returns its CommandFactory. Run calls it once,
// for the command it runs, before the bootstrap is sealed.
type Factory func(r forgo.Resolver) (CommandFactory, error)

// As returns a Factory that calls constructor with its parameters resolved by the bootstrap.
// The constructor returns a CommandFactory, optionally with an error.
func As(
	constructor any,
) Factory {
	return func(
		r forgo.Resolver,
	) (CommandFactory, error) {
		return r.Call[CommandFactory](
			constructor,
		)
	}
}
