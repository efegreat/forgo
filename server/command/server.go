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

const ServerNameCommand = "command"

type Server struct {
	cfg      Config
	handlers map[string]Handler

	locker operation.ILocker

	sem    process.ISemaphore
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Server) Term() forgo.TermConfig {
	return c.cfg.Term
}

func (c *Server) Name() string {
	return ServerNameCommand
}

func (c *Server) Init(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)
	for _, exec := range c.cfg.executions {
		c.handlers[exec.Name] = Handler{handler: c.createHandler(ctx, exec), concurrent: exec.Concurrent}
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

func (c *Server) createHandler(ctx context.Context, ex resolvedExecution) func([]string) {
	var middlewares []operation.Middleware
	if !c.cfg.DisableBaseMiddlewares {
		middlewares = append(middlewares,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Locker(c.locker, func(*operation.Context) operation.Lock { return ex.Lock }, nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Recovery(nil),
		)
	}
	handler := func(ctx *operation.Context) error {
		exec := ex.NewExec()
		fs := flag.NewFlagSet(ex.Name, flag.ContinueOnError)
		exec.Init(fs)

		fields := ctx.State.(*State).Fields

		if err := fs.Parse(fields[1:]); err != nil {
			return operation.Error{Code: "command_parse"}.Wrap(err)
		}
		if err := exec.Validate(); err != nil {
			return operation.Error{Code: "command_validate"}.Wrap(err)
		}

		return exec.Run(ctx)
	}
	handler = operation.Chain(handler, append(middlewares, c.cfg.ResolvedMiddlewares()...)...)
	return func(fields []string) {
		opCtx := &operation.Context{
			Context: context.WithoutCancel(ctx),
			ID:      "",
			Name:    ex.Name,
			Type:    OperationTypeCommand,
			Args:    types.NewMap[any, any](),
			State:   &State{Fields: fields},
		}
		handler(opCtx)
	}
}
