package cli_test

import (
	"context"
	"flag"
	"fmt"

	"github.com/efegreat/forgo/cli"
)

type greetCommand struct {
	name string
}

func (c *greetCommand) Init(fs *flag.FlagSet) {
	fs.StringVar(&c.name, "name", "world", "who to greet")
}

func (c *greetCommand) Validate() error { return nil }

func (c *greetCommand) Run(ctx context.Context) error {
	fmt.Println("hello,", c.name)
	return nil
}

// NewGreetCommand is resolved by the bootstrap, so it can take providers and resources as
// parameters. Each run gets a fresh command from the factory it returns.
func NewGreetCommand() cli.CommandFactory {
	return func() cli.Command { return &greetCommand{} }
}

func ExampleNew() {
	tool := cli.New(
		cli.WithName("tool"),
		cli.WithConfig(cli.Config{
			Commands: []cli.CommandConfig{
				{Name: "greet", Command: cli.As(NewGreetCommand)},
			},
		}),
		// The command line, which Run reads from os.Args by default.
		cli.WithArgs([]string{"tool", "greet", "-name", "Ada"}),
	)

	// Logs go to stderr, so that stdout holds the command's output only.
	if err := tool.Run(); err != nil {
		fmt.Println(err)
	}
	// Output: hello, Ada
}
