package openapi

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"
	"github.com/efegreat/forgo/transport"
)

type testSerializer struct {
	id string
	w  http.ResponseWriter
}

func testFactory(id string, w http.ResponseWriter) transport.ISerializer {
	return &testSerializer{id, w}
}
func (s *testSerializer) write(status int, value any) error {
	s.w.Header().Set("Content-Type", "application/json; charset=utf-8")
	s.w.Header().Set("X-Request-ID", s.id)
	s.w.WriteHeader(status)
	return json.NewEncoder(s.w).Encode(value)
}
func (s *testSerializer) Success(result any) error {
	return s.write(200, struct {
		ReqID  string `json:"reqId"`
		Status int    `json:"status"`
		Result any    `json:"result,omitempty"`
	}{s.id, 200, result})
}
func (s *testSerializer) Error(err error) error {
	var oe operation.Error
	if !errors.As(err, &oe) {
		return err
	}
	if err := s.write(400, map[string]any{"reqId": s.id, "status": 400, "errCode": oe.Code, "errMsg": oe.Message}); err != nil {
		return err
	}
	return oe // matches the supplied default serializer's behavior
}

type productRequest struct {
	ProductID uint64  `uri:"product_id" json:"productId" validate:"required"`
	Name      string  `json:"name" validate:"required,min=2"`
	Price     float64 `json:"price" validate:"gt=0"`
	Currency  string  `query:"currency" json:"currency" validate:"required,len=3"`
	RequestID string  `header:"X-Request-ID" validate:"required"`
	Ignored   string
}
type productResponse struct {
	ID          uint64             `json:"id"`
	Description string             `json:"description,omitempty"`
	Children    []*productResponse `json:"children,omitempty"`
}

func routeFor[Req, Resp any](name, method, path string) fhttp.RouteConfig {
	return fhttp.RouteConfig{Name: name, Method: method, Path: path,
		Handler: fhttp.HandlerFor(func(_ struct{}, _ context.Context, _ Req) (Resp, error) {
			panic("generator must never execute handler")
		})}
}
func productConfig() fhttp.Config {
	route := routeFor[productRequest, productResponse]("updateProduct", "PUT", "/products/{product_id:[0-9]{2,}}")
	route.Description = "Update a product"
	return fhttp.Config{Serializer: testFactory, Routes: []fhttp.RouteConfig{
		route, {Name: "health", Description: "Health check endpoint", Method: "GET", Path: "/health", Handler: fhttp.EmptyHandler()},
	}}
}
func asJSON(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestGenerateProductAndEmpty(t *testing.T) {
	doc, err := New(productConfig()).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	op := doc.Paths["/products/{product_id}"]["put"]
	if op.Description != "Update a product" || len(op.Parameters) != 3 {
		t.Fatalf("wrong operation: %+v", op)
	}
	props := op.RequestBody.Content["application/json"].Schema["properties"].(map[string]Schema)
	if len(props) != 2 || props["name"] == nil || props["price"] == nil {
		t.Fatalf("unexpected body properties: %v", props)
	}
	if !op.RequestBody.Required {
		t.Fatal("body should be required")
	}
	for _, param := range op.Parameters {
		if !param.Required {
			t.Fatalf("missing required: %s", param.Name)
		}
	}
	envelope := op.Responses["200"].Content["application/json"].Schema
	ref := envelope["properties"].(map[string]Schema)["result"]["$ref"].(string)
	model := doc.Components.Schemas[strings.TrimPrefix(ref, "#/components/schemas/")]
	if model["properties"].(map[string]Schema)["description"] == nil {
		t.Fatal("omitempty field lost")
	}
	if strings.Contains(string(mustJSON(t, doc)), markerKey) {
		t.Fatal("marker leaked")
	}
	health := doc.Paths["/health"]["get"]
	if health.RequestBody != nil || len(health.Parameters) != 0 {
		t.Fatal("empty handler has request")
	}
	if _, ok := health.Responses["200"].Content["application/json"].Schema["properties"].(map[string]Schema)["result"]; ok {
		t.Fatal("empty handler has invented result")
	}
	if op.Responses["default"].Content["application/json"].Schema["properties"].(map[string]Schema)["errCode"] == nil {
		t.Fatal("error scaffold missing")
	}
	if _, ok := op.Responses["200"].Headers["X-Request-Id"]; !ok {
		t.Fatal("response header missing")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type customSerializer struct {
	w            http.ResponseWriter
	panicSuccess bool
	discard      bool
}

func (s *customSerializer) Success(v any) error {
	if s.panicSuccess {
		panic("typed result rejected")
	}
	s.w.Header().Set("Content-Type", "application/vnd.example+json; charset=utf-8")
	s.w.WriteHeader(201)
	if s.discard {
		v = "discarded"
	}
	return json.NewEncoder(s.w).Encode(map[string]any{"payload": []any{map[string]any{"data": v}}})
}
func (s *customSerializer) Error(err error) error {
	s.w.Header().Set("Content-Type", "text/plain")
	s.w.WriteHeader(500)
	_, e := s.w.Write([]byte("error"))
	return e
}

func TestCustomSerializer(t *testing.T) {
	cfg := productConfig()
	cfg.Serializer = func(_ string, w http.ResponseWriter) transport.ISerializer { return &customSerializer{w: w} }
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	response := doc.Paths["/products/{product_id}"]["put"].Responses["201"]
	if len(response.Content) != 1 || response.Content["application/vnd.example+json"].Schema == nil {
		t.Fatal("wrong media type")
	}
	if !strings.Contains(string(mustJSON(t, response)), "$ref") {
		t.Fatal("nested marker was not replaced")
	}
}

func TestFailureCases(t *testing.T) {
	for _, tc := range []struct {
		name     string
		change   func(*fhttp.Config)
		contains string
	}{
		{"nil factory", func(c *fhttp.Config) { c.Serializer = nil }, "Serializer is nil"},
		{"missing URI", func(c *fhttp.Config) { c.Routes[0].Path = "/products/{other}" }, "uri parameter"},
		{"duplicate route", func(c *fhttp.Config) { c.Routes = append(c.Routes, c.Routes[0]) }, "duplicate operation"},
		{"duplicate name", func(c *fhttp.Config) { c.Routes[1].Name = c.Routes[0].Name }, "duplicate route name"},
		{"panic", func(c *fhttp.Config) {
			c.Serializer = func(_ string, w http.ResponseWriter) transport.ISerializer {
				return &customSerializer{w: w, panicSuccess: true}
			}
		}, "panicked"},
		{"discarded marker", func(c *fhttp.Config) {
			c.Serializer = func(_ string, w http.ResponseWriter) transport.ISerializer {
				return &customSerializer{w: w, discard: true}
			}
		}, "did not preserve"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := productConfig()
			tc.change(&cfg)
			_, err := New(cfg).Generate(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("got %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(productConfig()).Generate(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type EmbeddedA struct {
	Clash string
	Name  string `json:"name"`
}
type EmbeddedB struct {
	Clash string
}
type embeddedModel struct {
	EmbeddedA
	EmbeddedB
	Name int `json:"name"`
}
type nestedInput struct {
	Name  string `json:"name"`
	Token string `header:"X-Token"`
}
type nestedRequest struct {
	Input   nestedInput `json:"input"`
	Filters struct {
		Page int `query:"page"`
	}
	Excluded string `json:"-"`
}

func TestEmbeddingAndNestedSources(t *testing.T) {
	b := newSchemaBuilder(make(map[string]Schema))
	s, err := b.object(reflect.TypeFor[embeddedModel](), false)
	if err != nil {
		t.Fatal(err)
	}
	props := s["properties"].(map[string]Schema)
	if len(props) != 1 || props["name"]["type"] != "integer" {
		t.Fatalf("embedding dominance: %v", props)
	}
	params, body, err := inspectRequest(b, reflect.TypeFor[nestedRequest](), map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if len(params) != 2 || len(body.Content["application/json"].Schema["properties"].(map[string]Schema)) != 1 {
		t.Fatal("wrong nested source handling")
	}
	for _, s := range b.components {
		if props, ok := s["properties"].(map[string]Schema); ok && props["Token"] != nil {
			t.Fatal("header included in JSON")
		}
	}
}

func TestValidationPrecisionAndDive(t *testing.T) {
	b := newSchemaBuilder(make(map[string]Schema))
	s, _, err := b.validation(Schema{"type": "integer"}, reflect.TypeFor[uint64](), "lte=18446744073709551615")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mustJSON(t, s)), "18446744073709551615") {
		t.Fatal("lost uint64 precision")
	}
	s, required, err := b.validation(Schema{"type": "array"}, reflect.TypeFor[[]string](), "min=2,dive,required,len=4")
	if err != nil {
		t.Fatal(err)
	}
	if required || strings.Contains(string(mustJSON(t, s)), "maxItems") {
		t.Fatal("element validation leaked to collection")
	}
	values, err := oneofValues("'New York' Paris")
	if err != nil || !reflect.DeepEqual(values, []string{"New York", "Paris"}) {
		t.Fatal(values, err)
	}
}

func TestCommandAndAtomicOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "api.yaml")
	c := NewCommand(productConfig())
	set := flag.NewFlagSet("openapi", flag.ContinueOnError)
	c.Init(set)
	if err := set.Parse([]string{"-o=" + path}); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(data), `"openapi": "3.1.0"`) {
		t.Fatal(string(data), err)
	}
	c.generator.config.Serializer = nil
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("expected generation failure")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(data) {
		t.Fatal("failed generation damaged output")
	}
	if err := set.Parse([]string{"-format=xml"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err == nil {
		t.Fatal("invalid format accepted")
	}
}

func TestDeterministicOutput(t *testing.T) {
	var previous []byte
	for i := 0; i < 10; i++ {
		doc, err := New(productConfig()).Generate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		data, err := Marshal(doc, "yaml")
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && string(data) != string(previous) {
			t.Fatal("unstable output")
		}
		previous = data
	}
}

func TestExportExample(t *testing.T) {
	dir := os.Getenv("FORGO_OPENAPI_EXAMPLE_DIR")
	if dir == "" {
		t.Skip("example export not requested")
	}
	docs, err := New(versionedConfig(), WithInfo(Info{Title: "API Doc", Description: "HTTP API Doc"})).GenerateAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "yaml"} {
		if err := WriteFiles(context.Background(), filepath.Join(dir, "openapi."+format), format, docs); err != nil {
			t.Fatal(err)
		}
	}
}

func versionedConfig() fhttp.Config {
	cfg := productConfig()
	for i := range cfg.Routes {
		cfg.Routes[i].Version = "v1.0"
	}
	health := cfg.Routes[1]
	health.Version = "v2.0"
	cfg.Routes = append(cfg.Routes, health,
		fhttp.RouteConfig{Name: "ready", Method: "GET", Path: "/ready", Handler: fhttp.EmptyHandler()})
	return cfg
}

func TestGenerateAllVersions(t *testing.T) {
	docs, err := New(versionedConfig(), WithInfo(Info{Title: "API Doc", Description: "HTTP API Doc"})).GenerateAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 3 {
		t.Fatalf("expected 3 version groups, got %d", len(docs))
	}
	for version, doc := range docs {
		if doc.Info.Version != version || doc.Info.Title != "API Doc" || doc.Info.Description != "HTTP API Doc" {
			t.Fatalf("wrong info for %q: %+v", version, doc.Info)
		}
		encodedInfo := asJSON(t, doc.Info)
		if encodedInfo["version"] != version {
			t.Fatalf("missing/wrong wire version: %v", encodedInfo)
		}
	}
	if docs["v1.0"].Paths["/v1.0/health"]["get"] == nil || docs["v1.0"].Paths["/v1.0/products/{product_id}"]["put"] == nil {
		t.Fatal("version prefix missing")
	}
	if len(docs["v1.0"].Paths) != 2 || len(docs["v2.0"].Paths) != 1 || len(docs[""].Paths) != 1 {
		t.Fatal("routes leaked between versions")
	}
	if docs["v2.0"].Paths["/v2.0/health"]["get"] == nil || docs[""].Paths["/ready"]["get"] == nil {
		t.Fatal("wrong versioned/unversioned path")
	}
	if len(docs["v1.0"].Components.Schemas) == 0 || len(docs["v2.0"].Components.Schemas) != 0 {
		t.Fatal("components leaked between versions")
	}
	if _, err := New(versionedConfig()).Generate(context.Background()); err == nil || !strings.Contains(err.Error(), "GenerateAll") {
		t.Fatalf("mixed-version Generate should explain GenerateAll: %v", err)
	}
	cfg := productConfig()
	for i := range cfg.Routes {
		cfg.Routes[i].Version = "v1.0"
	}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil || doc.Info.Version != "v1.0" {
		t.Fatalf("single version Generate failed: %v", err)
	}
}

func TestVersionedCommandOutput(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "openapi.json")
	c := NewCommand(versionedConfig(), WithInfo(Info{Title: "API Doc", Description: "HTTP API Doc"}))
	set := flag.NewFlagSet("openapi", flag.ContinueOnError)
	c.Init(set)
	if err := set.Parse([]string{"-o=" + base}); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", "v1.0", "v2.0"} {
		path := VersionOutputPath(base, version)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var doc Document
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if doc.Info.Version != version {
			t.Fatalf("wrong output version in %s", path)
		}
	}
	before, _ := os.ReadFile(VersionOutputPath(base, "v1.0"))
	c.generator.config.Routes[2].Path = "/health/{missing}"
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("expected version generation failure")
	}
	after, _ := os.ReadFile(VersionOutputPath(base, "v1.0"))
	if string(before) != string(after) {
		t.Fatal("later version generation failure changed earlier file")
	}
}

func TestVersionOutputPath(t *testing.T) {
	for _, tc := range []struct{ base, version, want string }{
		{"docs/openapi.json", "v1.0", "docs/openapi-v1.0.json"},
		{"docs/openapi.yaml", "v2.0", "docs/openapi-v2.0.yaml"},
		{"docs/openapi.json", "", "docs/openapi.json"},
		{"docs/openapi", "v1.0", "docs/openapi-v1.0"},
		{"docs/openapi.json", "v1/test", "docs/openapi-v1%2Ftest.json"},
		{"docs/openapi.json", "v1%2Ftest", "docs/openapi-v1%252Ftest.json"},
	} {
		if got := VersionOutputPath(tc.base, tc.version); got != tc.want {
			t.Fatalf("got %q, want %q", got, tc.want)
		}
	}
}

func TestGenerateAllCancellationAndNoRoutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(versionedConfig()).GenerateAll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := New(fhttp.Config{Serializer: testFactory}).GenerateAll(context.Background()); err == nil {
		t.Fatal("empty config silently accepted")
	}
}

type failedErrorSerializer struct{ *testSerializer }

func (s *failedErrorSerializer) Error(err error) error {
	_ = s.testSerializer.Error(err)
	return fmt.Errorf("write failed: %w", err)
}

func TestErrorWriteFailureIsNotSwallowed(t *testing.T) {
	cfg := productConfig()
	cfg.Serializer = func(id string, w http.ResponseWriter) transport.ISerializer {
		return &failedErrorSerializer{&testSerializer{id, w}}
	}
	_, err := New(cfg).Generate(context.Background())
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("write failure swallowed: %v", err)
	}
}

func TestStandardTypesAndCollections(t *testing.T) {
	b := newSchemaBuilder(make(map[string]Schema))
	for _, tc := range []struct {
		t        reflect.Type
		contains string
	}{
		{reflect.TypeFor[time.Time](), "date-time"},
		{reflect.TypeFor[json.Number](), "number"},
		{reflect.TypeFor[[]byte](), "base64"},
		{reflect.TypeFor[map[int]*productResponse](), "additionalProperties"},
		{reflect.TypeFor[[3]int](), `"minItems":3`},
	} {
		s, err := b.schema(tc.t, false)
		if err != nil || !strings.Contains(string(mustJSON(t, s)), tc.contains) {
			t.Fatalf("%s: %v, %v", tc.t, s, err)
		}
	}
	if _, err := b.schema(reflect.TypeFor[chan string](), false); err == nil {
		t.Fatal("unsupported JSON type accepted")
	}
	s, required, err := b.validation(nullable(Schema{"type": "string"}), reflect.TypeFor[*string](), "required")
	if err != nil || !required || !strings.Contains(string(mustJSON(t, s)), `"not"`) {
		t.Fatalf("required pointer accepts null: %v %v", s, err)
	}
}

func TestDeclaredEmptyResponseKeepsResult(t *testing.T) {
	cfg := fhttp.Config{Serializer: testFactory, Routes: []fhttp.RouteConfig{routeFor[struct{}, struct{}]("emptyValue", "POST", "/empty")}}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	op := doc.Paths["/empty"]["post"]
	if op.RequestBody != nil {
		t.Fatal("empty request invented body")
	}
	props := op.Responses["200"].Content["application/json"].Schema["properties"].(map[string]Schema)
	if props["result"] == nil {
		t.Fatal("declared empty struct response lost result")
	}
}

func TestHeadHasNoBody(t *testing.T) {
	cfg := productConfig()
	cfg.Routes[0].Method = "HEAD"
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	op := doc.Paths["/products/{product_id}"]["head"]
	for _, response := range op.Responses {
		if response.Content != nil {
			t.Fatal("HEAD advertises response body")
		}
	}
}

func TestRootResultSerializer(t *testing.T) {
	cfg := productConfig()
	cfg.Serializer = func(id string, w http.ResponseWriter) transport.ISerializer {
		return &rootSerializer{&testSerializer{id, w}}
	}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s := doc.Paths["/products/{product_id}"]["put"].Responses["200"].Content["application/json"].Schema
	if s["$ref"] == nil {
		t.Fatal("root result not reflected")
	}
}

type rootSerializer struct{ *testSerializer }

func (s *rootSerializer) Success(v any) error { return s.write(200, v) }
