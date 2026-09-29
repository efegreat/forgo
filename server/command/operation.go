package command

import (
	"context"
	"flag"
)

const OperationTypeCommand = "command"

// Execution is a command's execution logic that will be used for a single command.
type Execution interface {
	Init(*flag.FlagSet)
	Validate() error
	Run(context.Context) error
}

type ExecutionFactory func() Execution

// Handler is the wrapper for command execution.
type Handler struct {
	handler    func(fields []string)
	concurrent bool
}

// State contains command-specific state associated with an operation.
type State struct {
	Fields []string
}
