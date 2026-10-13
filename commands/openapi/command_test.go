package openapi

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/efegreat/forgo/cli"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"

	"github.com/rs/zerolog"
)

func TestCommandRejectsArguments(t *testing.T) {
	c := NewCommand(productConfig())
	set := flag.NewFlagSet("openapi", flag.ContinueOnError)
	c.Init(set)
	if err := set.Parse([]string{"-format=json", "docs/openapi.json"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err == nil || err.Error() != `openapi: unexpected arguments ["docs/openapi.json"]; name the output file with -o` {
		t.Fatalf("got %v", err)
	}
	if err := new(Command).Validate(); err == nil || !strings.Contains(err.Error(), "create it with NewCommand") {
		t.Fatalf("zero command: %v", err)
	}
	empty := NewCommand(productConfig())
	empty.output = " "
	if err := empty.Validate(); err == nil || !strings.Contains(err.Error(), "output path is empty") {
		t.Fatalf("empty output: %v", err)
	}
}

// TestDefaultCommandThroughCLI runs NewDefaultOpenAPICommand the way applications do, on a config without a
// serializer, so the document comes from the server's default serializer.
func TestDefaultCommandThroughCLI(t *testing.T) {
	cfg := fhttp.Config{Routes: []fhttp.RouteConfig{
		routeFor[productRequest, productResponse]("updateProduct", "PUT", "/products/{product_id}"),
		{Name: "health", Method: "GET", Path: "/health", Handler: fhttp.HealthHandler()},
	}}
	run := func(args ...string) error {
		return cli.New(
			cli.WithLogger(logger.Config{Level: zerolog.Disabled}),
			cli.WithConfig(cli.Config{Commands: []cli.CommandConfig{
				{Name: "openapi", Command: cli.As(NewDefaultOpenAPICommand(cfg))},
			}}),
			cli.WithArgs(append([]string{"app", "openapi"}, args...)),
		).Run()
	}
	path := filepath.Join(t.TempDir(), "openapi.json")
	if err := run("-o=" + path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Info != (Info{Title: defaultTitle, Description: defaultDescription, Version: defaultVersion}) {
		t.Fatalf("info %+v", doc.Info)
	}
	envelope := doc.Paths["/products/{product_id}"]["put"].Responses["200"].Content["application/json"].Schema
	props, _ := envelope["properties"].(map[string]any)
	for _, key := range []string{"reqId", "status", "result"} {
		if props[key] == nil {
			t.Fatalf("default envelope lacks %s: %v", key, envelope)
		}
	}
	var oe operation.Error
	if err := run("extra"); !errors.As(err, &oe) || oe.Code != "command_validate" || !strings.Contains(err.Error(), "unexpected arguments") {
		t.Fatalf("positional argument: %v", err)
	}
}
