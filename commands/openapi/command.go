package openapi

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/efegreat/forgo/cli"

	fhttp "github.com/efegreat/forgo/server/http"
)

const defaultOutput = "openapi.json"

// Command is the openapi command. It writes the documents of GenerateAll with WriteFiles, to the file base named
// by -o (or -output, default openapi.json), as JSON or YAML (-format, inferred from the extension of the output).
type Command struct {
	generator *Generator
	set       *flag.FlagSet
	output    string
	format    string
}

// NewDefaultOpenAPICommand returns the constructor, for cli.As, of the openapi command for conf with the default
// Info. Use NewCommand to pass options.
func NewDefaultOpenAPICommand(conf fhttp.Config) func() cli.CommandFactory {
	return func() cli.CommandFactory {
		return func() cli.Command {
			return NewCommand(conf)
		}
	}
}

// NewCommand returns a command that generates the documents of config with New(config, options...).
func NewCommand(config fhttp.Config, options ...Option) *Command {
	return &Command{generator: New(config, options...), output: defaultOutput}
}

// Init declares the -o, -output and -format flags on set, and keeps set to reject positional arguments.
func (c *Command) Init(set *flag.FlagSet) {
	c.set = set
	set.StringVar(&c.output, "o", defaultOutput, "output filename base (version suffix added for each document)")
	set.StringVar(&c.output, "output", defaultOutput, "output filename base (alias of -o)")
	set.StringVar(&c.format, "format", "", "json or yaml (default: infer from output extension)")
}

func (c *Command) resolvedFormat() string {
	if c.format != "" {
		return strings.ToLower(c.format)
	}
	switch strings.ToLower(filepath.Ext(c.output)) {
	case ".yaml", ".yml":
		return "yaml"
	default:
		return "json"
	}
}

// Validate rejects positional arguments, an empty output path and a format other than json or yaml.
func (c *Command) Validate() error {
	if c.generator == nil {
		return errors.New("openapi: command has no generator; create it with NewCommand")
	}
	if c.set != nil && c.set.NArg() > 0 {
		return fmt.Errorf("openapi: unexpected arguments %q; name the output file with -o", c.set.Args())
	}
	if strings.TrimSpace(c.output) == "" {
		return fmt.Errorf("openapi: output path is empty")
	}
	switch c.resolvedFormat() {
	case "json", "yaml":
		return nil
	default:
		return fmt.Errorf("openapi: unsupported format %q", c.format)
	}
}

// Run generates the documents and writes them. It validates the command first, so it also works without a CLI.
func (c *Command) Run(ctx context.Context) error {
	if err := c.Validate(); err != nil {
		return err
	}
	docs, err := c.generator.GenerateAll(ctx)
	if err != nil {
		return err
	}
	return WriteFiles(ctx, c.output, c.resolvedFormat(), docs)
}
