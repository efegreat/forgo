package openapi

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/cli"

	fhttp "github.com/efegreat/forgo/server/http"
)

const defaultOutput = "openapi.json"

// CommandConfig configures the openapi command that Factory builds.
type CommandConfig struct {
	// HTTP is the configuration to document: pass the value the HTTP server uses.
	HTTP fhttp.Config
	// Options configure the Generator: WithInfo, WithSchema and WithDocument.
	Options []Option
}

// Factory returns the cli.Factory of the openapi command that cfg describes:
//
//	{Name: "openapi", Command: openapi.Factory(openapi.CommandConfig{HTTP: config.HTTP})}
//
// It builds the Generator once, when the CLI resolves its command, and resolves nothing: the generator never
// needs the application's dependencies.
func Factory(cfg CommandConfig) cli.Factory {
	return func(forgo.Resolver) (cli.CommandFactory, error) {
		generator := New(cfg.HTTP, cfg.Options...)
		return func() cli.Command { return NewCommand(generator) }, nil
	}
}

// Command is the openapi command. It writes the documents of GenerateAll with WriteFiles, to the file base named
// by -o (or -output, default openapi.json), as JSON or YAML (-format, inferred from the extension of the output).
// A Command holds the flags of one run: create one per run with NewCommand, or let Factory do it.
type Command struct {
	generator *Generator
	set       *flag.FlagSet
	output    string
	format    string
}

// NewCommand returns an openapi command that generates the documents of generator.
func NewCommand(generator *Generator) *Command {
	return &Command{generator: generator, output: defaultOutput}
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
		return errors.New("openapi: command has no generator; create it with Factory or NewCommand")
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
