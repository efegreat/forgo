package openapi

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/jwtauth"
	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

type goldenItem struct {
	ID        string            `json:"id"`
	Price     money             `json:"price"`
	Tags      []string          `json:"tags,omitempty"`
	Attrs     map[string]string `json:"attrs,omitempty"`
	CreatedAt time.Time         `json:"createdAt"`
	Parent    *goldenItem       `json:"parent,omitempty"`
}

type goldenListRequest struct {
	Page   int         `query:"page" validate:"omitempty,min=1"`
	Status orderStatus `query:"status"`
	Filter struct {
		Tags []string `query:"tag"`
	}
}

type goldenCreateRequest struct {
	Locale string     `header:"Accept-Language" json:"locale"`
	Name   string     `json:"name" validate:"required,min=2,max=64"`
	Price  money      `json:"price" validate:"required"`
	Parent *string    `json:"parent" validate:"omitempty,uuid"`
	Tags   []string   `json:"tags" validate:"max=10,dive,min=1"`
	Item   goldenItem `json:"item"`
}

type goldenTraceParams struct {
	Trace []string `header:"X-Trace" validate:"max=3"`
}

// goldenConfig uses most of what the generator documents, on the server's default serializer.
func goldenConfig() fhttp.Config {
	noop := func() operation.Middleware { return func(next operation.Handler) operation.Handler { return next } }
	keys := jwtauth.Keys{{Key: make([]byte, 32)}}
	create := routeFor[goldenCreateRequest, goldenItem]("createItem", "POST", "/items")
	create.Version, create.Description = "v1", "Creates an item."
	create.Middlewares = []forgo.MiddlewareFactory{fhttp.JWTAuth(fhttp.JWTConfig{Keys: keys, Cookie: "session"})}
	list := routeFor[goldenListRequest, pageOf[goldenItem]]("listItems", "GET", "/items")
	list.Version = "v1"
	get := routeFor[multiSourceRequest, *goldenItem]("getItem", "GET", "/items/{id}")
	get.Version, get.Middlewares = "v1", []forgo.MiddlewareFactory{
		fhttp.MiddlewareAs[goldenTraceParams](noop),
		fhttp.JWTAuth(fhttp.JWTConfig{Keys: keys, Optional: true}),
	}
	return fhttp.Config{
		ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{
			fhttp.MiddlewareAs[tenantParams](noop),
			fhttp.RateLimit(fhttp.RateLimitConfig{}),
		}},
		Routes: []fhttp.RouteConfig{
			create, list, get,
			{Name: "health", Method: "GET", Path: "/health", Handler: fhttp.HealthHandler()},
			routeFor[remainderRequest, struct{}]("file", "GET", "/files/{path...}"),
			{Name: "home", Method: "HEAD", Path: "/{$}", Handler: fhttp.EmptyHandler()},
			{Name: "admin", Method: "POST", Path: "/admin", Handler: fhttp.EmptyHandler(), DisableAPIDoc: true},
		},
	}
}

func goldenOptions() []Option {
	return []Option{
		WithInfo(Info{Title: "Shop API", Description: "The shop.", Version: "2.1.0"}),
		WithSchema[money](Schema{"type": "string", "format": "decimal"}),
		WithSchema[orderStatus](Schema{"type": "string", "enum": []string{"open", "paid"}}),
		WithDocument(func(version string, doc *Document) error {
			doc.Servers = []Server{{URL: "https://api.example.com"}}
			if health := doc.Operation("health"); health != nil {
				health.Tags = []string{"ops"}
				doc.Tags = []Tag{{Name: "ops", Description: "Operations."}}
			}
			return nil
		}),
	}
}

// TestGolden locks the generated documents. Run go test ./commands/openapi -run TestGolden -update to rewrite
// them after an intended change, and review the diff.
func TestGolden(t *testing.T) {
	docs, err := New(goldenConfig(), goldenOptions()...).GenerateAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("documents %d", len(docs))
	}
	for version, doc := range docs {
		for _, format := range []string{"json", "yaml"} {
			path := VersionOutputPath(filepath.Join("testdata", "golden."+format), version)
			data, err := Marshal(doc, format)
			if err != nil {
				t.Fatal(err)
			}
			if *update {
				if err := os.WriteFile(path, data, 0644); err != nil {
					t.Fatal(err)
				}
				continue
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run with -update to create it)", err)
			}
			if string(data) != string(want) {
				t.Errorf("%s differs; run with -update and review the diff. Got:\n%s", path, data)
			}
			if format == "yaml" && !strings.Contains(string(data), `"version": "`) {
				t.Errorf("%s has no version", path)
			}
		}
	}
}

func TestWithDocument(t *testing.T) {
	var versions []string
	record := WithDocument(func(version string, doc *Document) error {
		versions = append(versions, version)
		if doc.Components.Schemas == nil || strings.Contains(string(mustJSON(t, doc)), `"#/components/schemas/@`) {
			t.Fatal("hook ran before the components were named")
		}
		return nil
	})
	if _, err := New(versionedConfig(), record).GenerateAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Join(versions, ",") != ",v1.0,v2.0" {
		t.Fatalf("hook versions %q", versions)
	}
	failure := errors.New("no servers")
	fail := WithDocument(func(string, *Document) error { return failure })
	if _, err := New(productConfig(), fail).Generate(context.Background()); !errors.Is(err, failure) ||
		!strings.HasPrefix(err.Error(), "openapi: document hook:") {
		t.Fatalf("hook error: %v", err)
	}
	doc, err := New(productConfig()).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if doc.Operation("health") != doc.Paths["/health"]["get"] || doc.Operation("missing") != nil {
		t.Fatal("Operation lookup")
	}
	// A nil Security inherits the document's, an empty one has none.
	if strings.Contains(string(mustJSON(t, doc)), `"security"`) {
		t.Fatal("nil security encoded")
	}
	doc.Operation("health").Security = []SecurityRequirement{}
	if !strings.Contains(string(mustJSON(t, doc)), `"security":[]`) {
		t.Fatal("empty security dropped")
	}
}

func TestNewDefaults(t *testing.T) {
	// Without Sanitize, New documents the default serializer.
	cfg := productConfig()
	cfg.Serializer = nil
	doc, err := New(cfg, WithInfo(Info{Description: "only a description"})).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if doc.Info != (Info{Title: defaultTitle, Description: "only a description", Version: defaultVersion}) {
		t.Fatalf("info %+v", doc.Info)
	}
	docs, err := New(versionedConfig(), WithInfo(Info{Title: "Shop", Version: "3.0.0"})).GenerateAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if docs[""].Info.Version != "3.0.0" || docs["v1.0"].Info.Version != "v1.0" || docs[""].Info.Description != "" {
		t.Fatalf("versions %+v %+v", docs[""].Info, docs["v1.0"].Info)
	}
}
