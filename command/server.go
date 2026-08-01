package command

import (
	"bufio"
	"context"
	"fmt"
	"forgo"
	"forgo/logger"
	"forgo/operation"
	"forgo/process"
	"os"
	"strings"
	"sync"

	"github.com/kballard/go-shellquote"
)

const (
	ServerNameCommand    = "command"
	DefaultMaxConcurrent = 10
)

type Server struct {
	*forgo.Server
	factories     []Factory
	handlers      map[string]Handler
	maxConcurrent int
	sem           process.ISemaphore
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewServer(opts ...ServerOption) *Server {
	cs := &Server{Server: &forgo.Server{}, handlers: make(map[string]Handler), maxConcurrent: DefaultMaxConcurrent}
	for _, opt := range opts {
		opt(cs)
	}
	if cs.factories == nil {
		cs.factories = make([]Factory, 0)
	}
	if cs.Server.Tt == 0 {
		cs.Server.Tt = forgo.DefaultServerTimeout
	}
	cs.sem = process.NewChannelSemaphore(cs.maxConcurrent)

	return cs
}

func (c *Server) Init(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	lg := logger.FromCtx(ctx)
	for _, f := range c.factories {
		fn := func(fields []string) {
			h := operation.NewHandler(*lg, c.Locker, Operation{cmd: f(), fields: fields})
			h()
		}
		cmd := f()
		c.handlers[cmd.Name()] = Handler{handler: fn, concurrent: cmd.Concurrent()}
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

type ServerOption func(*Server)

func WithBaseOption(opt forgo.ServerOption) ServerOption {
	return func(s *Server) {
		opt(s.Server)
	}
}

func WithFactory(f Factory) ServerOption {
	return func(cs *Server) {
		cs.factories = append(cs.factories, f)
	}
}

func WithMaxConcurrent(concurrent int) ServerOption {
	return func(cs *Server) {
		cs.maxConcurrent = concurrent
	}
}
