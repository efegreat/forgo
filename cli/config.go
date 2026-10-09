package cli

import (
	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

type Config struct {
	DisableBaseMiddlewares bool
	Middlewares            []forgo.MiddlewareFactory
	Commands               []CommandConfig
}

type CommandConfig struct {
	Name        string
	Lock        operation.Lock
	Middlewares []forgo.MiddlewareFactory
	Command     Factory
}

type resolvedCommand struct {
	Name        string
	Lock        operation.Lock
	Middlewares []operation.Middleware
	Command     CommandFactory
}

type Factory func(r forgo.Resolver) (CommandFactory, error)

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
