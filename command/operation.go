package command

import (
	"context"
	"flag"
	"forgo/operation"
)

const OperationTypeCommand = "command"

// ICommand is an interface that defines the structure and behavior of a command. It includes methods for retrieving the command's name, concurrent execution setting, and an Invocation factory to be triggered.
type ICommand interface {
	Name() string
	Concurrent() bool
	Lock() operation.Lock
	NewInvocation() Invocation
}

// Invocation is a command's execution logic that will be used for a single command.
type Invocation interface {
	Init(*flag.FlagSet)
	Validate() error
	Run(context.Context) error
}

// Handler is the wrapper for command execution.
type Handler struct {
	handler    func(fields []string)
	concurrent bool
}

// State contains command-specific state associated with an operation.
type State struct {
	fields []string
}
