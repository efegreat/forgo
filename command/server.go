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
	"github.com/rs/zerolog"
)

const (
	ServerNameCommand    = "command"
	DefaultMaxConcurrent = 10
)

var fieldsCtxKey types.CtxKey

type Server struct {
	*forgo.Server
	commands      []ICommand
	handlers      map[string]Handler
	maxConcurrent int
	sem           process.ISemaphore
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewServer(opts ...ServerOption) *Server {
	cs := &Server{Server: &forgo.Server{}, commands: make([]ICommand, 0), handlers: make(map[string]Handler), maxConcurrent: DefaultMaxConcurrent}
	for _, opt := range opts {
		opt(cs)
	}
	if cs.Server.Tt == 0 {
		cs.Server.Tt = forgo.DefaultServerTimeout
	}
	cs.sem = process.NewChannelSemaphore(cs.maxConcurrent)

	return cs
}

func (c *Server) Init(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, cmd := range c.commands {
		c.handlers[cmd.Name()] = Handler{handler: c.createHandler(ctx, cmd), concurrent: cmd.Concurrent()}
	}

	return nil
}

func (c *Server) Name() string {
	return ServerNameCommand
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
	if !c.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(func(z zerolog.Context) zerolog.Context {
				return z
			}),
			middleware.Locker(c.Locker, cmd.Lock()),
			middleware.Recorder(func(event *zerolog.Event) *zerolog.Event {
				return event
			}, func(event *zerolog.Event) *zerolog.Event {
				return event
			}),
			middleware.Recovery(),
		)
	}
	handler := func(ctx *operation.Context) error {
		inv := cmd.NewInvocation()
		fs := flag.NewFlagSet(cmd.Name(), flag.ContinueOnError)
		inv.Init(fs)

		fields := ctx.Args.GetStringSlice(fieldsCtxKey)

		if err := fs.Parse(fields[1:]); err != nil {
			return operation.Error{Err: err}
		}
		if err := inv.Validate(); err != nil {
			return operation.Error{Err: err}
		}

		return inv.Run(ctx.Context)
	}
	handler = operation.Chain(handler, append(middlewares, c.Middlewares...)...)
	return func(fields []string) {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    cmd.Name(),
			Type:    operationTypeCommand,
			Meta: func(s string) string {
				return ""
			},
		}
		opCtx.Args.Set(fieldsCtxKey, fields)
		handler(opCtx)
	}
}

type ServerOption func(*Server)

func WithBaseOption(opt forgo.ServerOption) ServerOption {
	return func(s *Server) {
		opt(s.Server)
	}
}

func WithCommands(commands ...ICommand) ServerOption {
	return func(s *Server) {
		s.commands = append(s.commands, commands...)
	}
}

func WithMaxConcurrent(concurrent int) ServerOption {
	return func(cs *Server) {
		cs.maxConcurrent = concurrent
	}
}
