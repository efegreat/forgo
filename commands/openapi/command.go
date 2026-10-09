package openapi

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/efegreat/forgo/cli"

	fhttp "github.com/efegreat/forgo/server/http"
)

// Command implements the current Init/Validate/Run command contract.
// Naming and concurrency policies belong to the CLI registration layer.
type Command struct {
	generator *Generator
	output    string
	format    string
}

// NewDefaultOpenAPICommand returns the cli command that generates OpenAPI documents for the given HTTP config with default metadata.
func NewDefaultOpenAPICommand(conf fhttp.Config) func() cli.CommandFactory {
	return func() cli.CommandFactory {
		return func() cli.Command {
			return NewCommand(conf, WithInfo(Info{
				Title:       "API Doc",
				Description: "HTTP API Doc",
			}))
		}
	}
}

// NewCommand returns a command that generates OpenAPI documents for the given HTTP config.
func NewCommand(config fhttp.Config, options ...Option) *Command {
	return &Command{generator: New(config.Sanitize(), options...), output: "openapi.json"}
}

func (c *Command) Init(set *flag.FlagSet) {
	set.StringVar(&c.output, "o", "openapi.json", "output filename base (version suffix added for each document)")
	set.StringVar(&c.output, "output", "openapi.json", "output filename base (alias of -o)")
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

func (c *Command) Validate() error {
	if c.generator == nil {
		return fmt.Errorf("openapi: command has no generator")
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
