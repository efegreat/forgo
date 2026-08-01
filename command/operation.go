package command

import (
	"context"
	"flag"
	"forgo/operation"
)

const operationTypeCommand = "command"

type ICommand interface {
	Name() string
	Concurrent() bool
	Lock() operation.Lock
	Init(set *flag.FlagSet)
	Validate() error
	Run(context.Context) error
}

type Factory func() ICommand

type Operation struct {
	cmd    ICommand
	fields []string
}

func (c Operation) ID() string {
	return ""
}

func (c Operation) Name() string {
	return c.cmd.Name()
}

func (c Operation) Type() string {
	return operationTypeCommand
}

func (c Operation) Tolerance() operation.Tolerance {
	return operation.Tolerance{}
}

func (c Operation) Lock() operation.Lock {
	return c.cmd.Lock()
}

func (c Operation) BeforeRun(context.Context) operation.Error {
	fs := flag.NewFlagSet(c.cmd.Name(), flag.ContinueOnError)
	c.cmd.Init(fs)

	if err := fs.Parse(c.fields[1:]); err != nil {
		return operation.Error{Err: err}
	}
	if err := c.cmd.Validate(); err != nil {
		return operation.Error{Err: err}
	}

	return operation.Error{}
}

func (c Operation) Run(ctx context.Context) operation.Error {
	return operation.Error{
		Err: c.cmd.Run(ctx),
	}
}

type Handler struct {
	handler    func(fields []string)
	concurrent bool
}
