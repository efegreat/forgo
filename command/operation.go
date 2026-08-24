package command

import (
	"context"
	"flag"
	"forgo/operation"
)

const OperationTypeCommand = "command"

type ICommand interface {
	Name() string
	Concurrent() bool
	Lock() operation.Lock
	NewInvocation() Invocation
}

type Invocation interface {
	Init(*flag.FlagSet)
	Validate() error
	Run(context.Context) error
}

type Handler struct {
	handler    func(fields []string)
	concurrent bool
}

// State contains command-specific state associated with an operation.
type State struct {
	fields []string
}
