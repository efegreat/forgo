package forgo

import (
	"context"
	"flag"
	"fmt"
	"forgo/operation"
)

type ICommand interface {
	Name() string
	Init(set *flag.FlagSet)
	Validate() error
	Run(ctx context.Context) operation.Error
}

type CommandOperation struct {
	cmd  ICommand
	args []string
}

func (c CommandOperation) Name() string {
	return c.cmd.Name()
}

func (c CommandOperation) Type() operation.Type {
	return operation.TypeCommand
}

func (c CommandOperation) Tolerance() operation.Tolerance {
	return operation.Tolerance{}
}

func (c CommandOperation) Lock() operation.Lock {
	return operation.Lock{}
}

func (c CommandOperation) BeforeRun(context.Context) operation.Error {
	s := flag.NewFlagSet(c.cmd.Name(), flag.ExitOnError)
	c.cmd.Init(s)

	if err := s.Parse(c.args[2:]); err != nil {
		return operation.Error{
			Severity: operation.SeverityLight,
			Err:      err,
		}
	}
	if err := c.cmd.Validate(); err != nil {
		return operation.Error{
			Severity: operation.SeverityLight,
			Err:      fmt.Errorf("invalid flags for command %s: %w", c.cmd.Name(), err),
		}
	}

	return operation.Error{}
}

func (c CommandOperation) Run(ctx context.Context) operation.Error {
	return c.cmd.Run(ctx)
}

type CommandServer struct {
}
