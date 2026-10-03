package cli

import (
	"context"
	"flag"
)

const OperationTypeCommand = "command"

// Command is a command's execution logic that will be used for a single command.
type Command interface {
	Init(*flag.FlagSet)
	Validate() error
	Run(context.Context) error
}

type CommandFactory func() Command

// State contains command-specific state associated with an operation.
type State struct {
	Fields []string
}
