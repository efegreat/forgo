package command

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"forgo"
	"forgo/middleware"
	"forgo/operation"
	"forgo/process"
	"forgo/types"
	"os"
	"strings"
	"sync"

	"github.com/kballard/go-shellquote"
)

const (
	ServerNameCommand    = "command"
	defaultMaxConcurrent = 10
)

type Config struct {
	forgo.ServerConfig
	MaxConcurrent int
	Commands      []ICommand
}

func (c Config) Sanitize() Config {
	if c.MaxConcurrent < 1 {
		c.MaxConcurrent = defaultMaxConcurrent
	}
	if c.TermTimeout == 0 {
		c.TermTimeout = forgo.DefaultServerTimeout
	}
	return c
}

type Server struct {
	cfg      Config
	handlers map[string]Handler

	sem    process.ISemaphore
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewServer(cfg Config) *Server {
	return &Server{cfg: cfg.Sanitize(), handlers: make(map[string]Handler), sem: process.NewChannelSemaphore(cfg.MaxConcurrent)}
}

func (c *Server) Config() forgo.ServerConfig {
	return c.cfg.ServerConfig
}

func (c *Server) Name() string {
	return ServerNameCommand
}

func (c *Server) Init(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, cmd := range c.cfg.Commands {
		c.handlers[cmd.Name()] = Handler{handler: c.createHandler(ctx, cmd), concurrent: cmd.Concurrent()}
	}

	return nil
}

func (c *Server) Serve() error {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			fmt.Print("> ")

			if !scanner.Scan() {
				return scanner.Err()
			}

			line := strings.TrimSpace(scanner.Text())

			if line == "" {
				continue
			}

			c.dispatch(line)
		}
	}
}

func (c *Server) dispatch(line string) {
	fields, err := shellquote.Split(line)
	if err != nil {
		fmt.Println("Parse error:", err)
		return
	}

	if len(fields) == 0 {
		return
	}

	name := strings.ToLower(fields[0])

	h, ok := c.handlers[name]
	if !ok {
		var handlers string
		for k := range c.handlers {
			handlers += k + ","
		}

		handlers = handlers[:len(handlers)-1]
		fmt.Println("Available commands:", handlers)
		return
	}

	if h.concurrent {
		if err := c.sem.Acquire(); err != nil {
			fmt.Println(err)
			return
		}
		c.wg.Go(func() {
			defer c.sem.Release()
			h.handler(fields)
		})
		return
	}

	h.handler(fields)
}

func (c *Server) Terminate(context.Context) error {
	c.cancel()
	c.wg.Wait()
	return nil
}

func (c *Server) Kill() {}

func (c *Server) createHandler(ctx context.Context, cmd ICommand) func([]string) {
	var middlewares []operation.Middleware
	if !c.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(c.cfg.Locker, func(*operation.Context) operation.Lock { return cmd.Lock() }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Recovery(nil),
		)
	}
	handler := func(ctx *operation.Context) error {
		inv := cmd.NewInvocation()
		fs := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
		inv.Init(fs)

		fields := ctx.State.(*State).Fields

		if err := fs.Parse(fields[1:]); err != nil {
			return operation.Error{Code: "command_parse"}.Wrap(err)
		}
		if err := inv.Validate(); err != nil {
			return operation.Error{Code: "command_validate"}.Wrap(err)
		}

		return inv.Run(ctx)
	}
	handler = operation.Chain(handler, append(middlewares, c.cfg.Middlewares...)...)
	return func(fields []string) {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    cmd.Name(),
			Type:    OperationTypeCommand,
			Args:    types.NewMap(),
			State:   &State{Fields: fields},
		}
		handler(opCtx)
	}
}
