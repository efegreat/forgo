package cli

import (
	"context"
	"flag"
)

// OperationTypeCommand is the operation type of a command run.
const OperationTypeCommand = "command"

// Command is the logic of one command. Every run creates a fresh Command from its
// CommandFactory, so it can keep its flag values in its fields.
//
// CLI.Run calls Init with a new flag.FlagSet named after the command, parses the arguments after
// the command name, then calls Validate and, if it returns nil, Run. -h and -help print the usage
// and end the run successfully before Validate. Parse errors carry code command_parse and
// Validate errors carry code command_validate.
type Command interface {
	// Init declares the command's flags.
	Init(*flag.FlagSet)
	// Validate checks the parsed flags.
	Validate() error
	// Run does the work. Its context is the run's *operation.Context, which is cancelled on
	// SIGINT or SIGTERM.
	Run(context.Context) error
}

// CommandFactory creates a fresh Command for each run.
type CommandFactory func() Command

// State is the transport state of a command run.
type State struct {
	// Fields is the command line: the program name, the command name, then the command's
	// arguments. It is os.Args unless the CLI was created WithArgs.
	Fields []string
}
