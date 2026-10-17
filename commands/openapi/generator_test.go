package openapi

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/jwtauth"
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
	route := routeFor[productRequest, productResponse]("updateProduct", "PUT", "/products/{product_id}")
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
	body := op.RequestBody.Content["application/json"].Schema
	props := body["properties"].(map[string]Schema)
	// productId and currency are bound from the body too, so the body documents them.
	if len(props) != 4 || props["name"] == nil || props["price"] == nil || props["productId"] == nil || props["currency"] == nil {
		t.Fatalf("unexpected body properties: %v", props)
	}
	if !op.RequestBody.Required || !slices.Equal(body["required"].([]string), []string{"name"}) {
		t.Fatalf("body required %v", body["required"])
	}
	if note := props["currency"]["description"]; note != `Also accepted as the query parameter "currency". When a request sends several, the query parameter "currency" wins.` {
		t.Fatalf("currency body note %q", note)
	}
	for _, param := range op.Parameters {
		// The path is always required. A currency in the body satisfies required, so the query parameter is optional.
		if param.Required != (param.Name != "currency") {
			t.Fatalf("parameter %s required %v", param.Name, param.Required)
		}
	}
	if note := paramsByName(op)["query:currency"].Description; note != `Also accepted as the body property "currency". When a request sends several, this one wins.` {
		t.Fatalf("currency parameter note %q", note)
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
	// Only the transport's X-Request-ID is read.
	if health.RequestBody != nil || len(health.Parameters) != 1 {
		t.Fatalf("empty handler has request: %+v", health.Parameters)
	}
	if p := health.Parameters[0]; p.Name != "X-Request-Id" || p.In != "header" || p.Required ||
		p.Description != requestIDDescription {
		t.Fatalf("request ID parameter %+v", p)
	}
	// A declared X-Request-ID keeps its schema and gets the description.
	if p := paramsByName(op)["header:X-Request-Id"]; !p.Required || p.Description != requestIDDescription {
		t.Fatalf("declared request ID parameter %+v", p)
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
	if _, err := new(Generator).Generate(context.Background()); err == nil || !strings.Contains(err.Error(), "create it with New") {
		t.Fatalf("zero generator: %v", err)
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
	b := newSchemaBuilder(nil)
	s, err := b.object(reflect.TypeFor[embeddedModel](), false)
	if err != nil {
		t.Fatal(err)
	}
	props := s["properties"].(map[string]Schema)
	if len(props) != 1 || props["name"]["type"] != "integer" {
		t.Fatalf("embedding dominance: %v", props)
	}
	params, body, err := inspectRequest(b, reflect.TypeFor[nestedRequest](), map[string]bool{}, map[string]bool{})
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
	b := newSchemaBuilder(nil)
	s, _, err := b.validation(Schema{"type": "integer"}, reflect.TypeFor[uint64](), "lte=18446744073709551615", true)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mustJSON(t, s)), "18446744073709551615") {
		t.Fatal("lost uint64 precision")
	}
	s, required, err := b.validation(Schema{"type": "array"}, reflect.TypeFor[[]string](), "min=2,dive,required,len=4", true)
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
		want := cmp.Or(version, defaultVersion) // the unversioned document takes Info.Version
		if doc.Info.Version != want || doc.Info.Title != "API Doc" || doc.Info.Description != "HTTP API Doc" {
			t.Fatalf("wrong info for %q: %+v", version, doc.Info)
		}
		encodedInfo := asJSON(t, doc.Info)
		if encodedInfo["version"] != want {
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
		if want := cmp.Or(version, defaultVersion); doc.Info.Version != want {
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
	b := newSchemaBuilder(nil)
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
	s, required, err := b.validation(nullable(Schema{"type": "string"}), reflect.TypeFor[*string](), "required", true)
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

type tenantParams struct {
	TenantID  string `header:"x-tenant-id" validate:"required,uuid"`
	RequestID string `header:"X-Request-ID"`
}
type localeParams struct {
	Locale    string `query:"locale" validate:"omitempty,oneof=en tr"`
	RequestID string `header:"X-Request-ID" validate:"required"`
}
type tenantStore struct{}

func paramsByName(op *Operation) map[string]Parameter {
	params := make(map[string]Parameter)
	for _, p := range op.Parameters {
		params[p.In+":"+p.Name] = p
	}
	return params
}

func TestMiddlewareParams(t *testing.T) {
	constructed := 0
	construct := func() operation.Middleware {
		constructed++
		return func(next operation.Handler) operation.Handler { return next }
	}
	cfg := productConfig()
	cfg.Middlewares = []forgo.MiddlewareFactory{
		fhttp.MiddlewareAs[tenantParams](construct),
		forgo.MiddlewareAs(func(*tenantStore) operation.Middleware { panic("generator must never construct middleware") }),
	}
	cfg.Routes[1].Middlewares = []forgo.MiddlewareFactory{fhttp.MiddlewareAs[*localeParams](construct)}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if constructed != 0 {
		t.Fatalf("generator constructed %d middlewares", constructed)
	}

	product := doc.Paths["/products/{product_id}"]["put"]
	var order []string
	for _, p := range product.Parameters {
		order = append(order, p.In+":"+p.Name)
	}
	if want := []string{"header:X-Request-Id", "header:X-Tenant-Id", "path:product_id", "query:currency"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("product parameters %v, want %v", order, want)
	}
	params := paramsByName(product)
	// The handler requires X-Request-ID and the middleware does not: the merged parameter is required.
	if !params["header:X-Request-Id"].Required || !params["header:X-Tenant-Id"].Required ||
		!strings.Contains(string(mustJSON(t, params["header:X-Tenant-Id"].Schema)), `"format":"uuid"`) {
		t.Fatalf("server middleware params: %+v", params)
	}

	// The server middleware does not require X-Request-ID and the route middleware does.
	health := paramsByName(doc.Paths["/health"]["get"])
	if len(health) != 3 || !health["header:X-Request-Id"].Required || health["query:locale"].Required ||
		!strings.Contains(string(mustJSON(t, health["query:locale"].Schema)), `"enum":["en","tr"]`) {
		t.Fatalf("health parameters: %+v", health)
	}
	if doc.Paths["/health"]["get"].RequestBody != nil {
		t.Fatal("middleware params invented a body")
	}
}

func TestMiddlewareParamsCoverPath(t *testing.T) {
	type tenantPath struct {
		Tenant string `uri:"tenant"`
	}
	noop := func() operation.Middleware { return func(next operation.Handler) operation.Handler { return next } }
	route := fhttp.RouteConfig{Name: "tenantHealth", Method: "GET", Path: "/tenants/{tenant}/health", Handler: fhttp.EmptyHandler()}
	cfg := fhttp.Config{Serializer: testFactory, Routes: []fhttp.RouteConfig{route}}
	if _, err := New(cfg).Generate(context.Background()); err == nil || !strings.Contains(err.Error(), "no matching uri tag") {
		t.Fatalf("uncovered wildcard: %v", err)
	}
	cfg.Routes[0].Middlewares = []forgo.MiddlewareFactory{fhttp.MiddlewareAs[tenantPath](noop)}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p := paramsByName(doc.Paths["/tenants/{tenant}/health"]["get"])["path:tenant"]; !p.Required {
		t.Fatalf("path parameter from middleware: %+v", p)
	}
}

func TestMiddlewareParamsFailures(t *testing.T) {
	noop := func() operation.Middleware { return func(next operation.Handler) operation.Handler { return next } }
	for _, tc := range []struct {
		name     string
		server   bool
		factory  forgo.MiddlewareFactory
		contains string
	}{
		{"conflicting schema", false, fhttp.MiddlewareAs[struct {
			Currency int `query:"currency"`
		}](noop), `query parameter "currency" is declared with a different schema`},
		{"json field", false, fhttp.MiddlewareAs[struct {
			Name string `json:"name"`
		}](noop), "json fields are not supported"},
		{"not a struct", false, fhttp.MiddlewareAs[string](noop), "params type must be a struct, got string"},
		{"duplicate parameter", false, fhttp.MiddlewareAs[struct {
			A string `header:"X-Tenant"`
			B string `header:"x-tenant"`
		}](noop), `duplicate header parameter "X-Tenant"`},
		{"path parameter missing on a route", true, fhttp.MiddlewareAs[struct {
			ID uint64 `uri:"product_id"`
		}](noop), `route "health" (GET /health): middleware params`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := productConfig()
			if tc.server {
				cfg.Middlewares = []forgo.MiddlewareFactory{tc.factory}
			} else {
				cfg.Routes[0].Middlewares = []forgo.MiddlewareFactory{tc.factory}
			}
			_, err := New(cfg).Generate(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("got %v, want %q", err, tc.contains)
			}
		})
	}
}

type liveCheckout struct {
	Enabled bool `json:"enabled"`
}

func (liveCheckout) Name() string          { return "checkout" }
func (liveCheckout) Default() liveCheckout { return liveCheckout{} }

func TestDisableAPIDoc(t *testing.T) {
	catalog := forgo.LiveConfigCatalog{forgo.LiveConfigOf[liveCheckout]()}
	routes := []fhttp.RouteConfig{
		{Name: "live_configs", Method: "GET", Path: "/admin/configs", Handler: fhttp.LiveConfigsHandler(catalog), DisableAPIDoc: true},
		{Name: "set_live_config", Method: "PUT", Path: "/admin/configs/{name}", Handler: fhttp.SetLiveConfigHandler(catalog), DisableAPIDoc: true},
		{Name: "ready", Method: "GET", Path: "/ready", Handler: fhttp.EmptyHandler()},
		// A version whose routes are all hidden gets no document.
		{Name: "private", Method: "GET", Path: "/private", Version: "v9", Handler: fhttp.EmptyHandler(), DisableAPIDoc: true},
	}

	doc, err := New(fhttp.Config{Serializer: testFactory, Routes: routes[:3]}).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.Paths) != 1 || doc.Paths["/ready"]["get"] == nil {
		t.Fatalf("hidden routes documented: %v", slices.Collect(maps.Keys(doc.Paths)))
	}

	docs, err := New(fhttp.Config{Serializer: testFactory, Routes: routes}).GenerateAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := docs["v9"]; ok || len(docs) != 1 || len(docs[""].Paths) != 1 {
		t.Fatalf("documents %v", slices.Collect(maps.Keys(docs)))
	}

	// Documented, the live config routes have their path wildcard and If-Match header.
	for i := range routes[:2] {
		routes[i].DisableAPIDoc = false
	}
	doc, err = New(fhttp.Config{Serializer: testFactory, Routes: routes[:2]}).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	put := doc.Paths["/admin/configs/{name}"]["put"]
	if doc.Paths["/admin/configs"]["get"] == nil || put == nil {
		t.Fatalf("live config routes missing: %v", slices.Collect(maps.Keys(doc.Paths)))
	}
	var params []string
	for _, p := range put.Parameters {
		if p.Required {
			params = append(params, p.In+":"+p.Name)
		}
	}
	if !slices.Equal(params, []string{"header:If-Match", "path:name"}) {
		t.Fatalf("set parameters %v", params)
	}
}

// TestMiddlewareContractsDocumented checks the security, responses and headers documented from the contracts of
// JWTAuth and RateLimitWith, whose limiter the generator never resolves.
func TestMiddlewareContractsDocumented(t *testing.T) {
	keys := jwtauth.Keys{{Key: make([]byte, 32)}}
	auth := fhttp.JWTAuth(fhttp.JWTConfig{Keys: keys, Cookie: "my session"})
	open := routeFor[struct{}, struct{}]("open", "GET", "/open")
	secured := routeFor[struct{}, struct{}]("secured", "GET", "/secured")
	secured.Middlewares = []forgo.MiddlewareFactory{auth}
	both := routeFor[struct{}, struct{}]("both", "GET", "/both")
	both.Middlewares = []forgo.MiddlewareFactory{auth, fhttp.JWTAuth(fhttp.JWTConfig{Keys: keys, Optional: true})}
	cfg := fhttp.Config{
		ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{fhttp.RateLimitWith(fhttp.RateLimitConfig{})}},
		Routes:       []fhttp.RouteConfig{open, secured, both},
	}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	wantSchemes := map[string]SecurityScheme{
		"bearerAuth": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
		"my_session": {Type: "apiKey", In: "cookie", Name: "my session"},
	}
	if !maps.Equal(doc.Components.SecuritySchemes, wantSchemes) {
		t.Fatalf("schemes %+v", doc.Components.SecuritySchemes)
	}
	for id, want := range map[string]string{
		"open":    `null`,
		"secured": `[{"bearerAuth":[]},{"my_session":[]}]`,
		// Both middlewares authenticate: the second takes a bearer token or none.
		"both": `[{"bearerAuth":[]},{"bearerAuth":[],"my_session":[]},{"my_session":[]}]`,
	} {
		if got := string(mustJSON(t, doc.Operation(id).Security)); got != want {
			t.Errorf("%s security %s, want %s", id, got, want)
		}
	}

	headers := func(op *Operation, status string) []string {
		return slices.Sorted(maps.Keys(op.Responses[status].Headers))
	}
	rateLimit := []string{"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset", "X-Request-Id"}
	for _, id := range []string{"open", "secured"} {
		op := doc.Operation(id)
		for _, status := range []string{"200", "default"} {
			if got := headers(op, status); !slices.Equal(got, rateLimit) {
				t.Errorf("%s %s headers %v", id, status, got)
			}
		}
		if got := headers(op, "429"); !slices.Equal(got, slices.Insert(slices.Clone(rateLimit), 3, "Retry-After")) {
			t.Errorf("%s 429 headers %v", id, got)
		}
		if op.Responses["429"].Description != "Too Many Requests" || op.Responses["429"].Content == nil {
			t.Errorf("%s 429 %+v", id, op.Responses["429"])
		}
	}
	if _, ok := doc.Operation("open").Responses["401"]; ok {
		t.Error("401 without JWTAuth")
	}
	// The rate limit runs first, so its headers are on the 401 too.
	unauthorized := doc.Operation("secured").Responses["401"]
	if got := headers(doc.Operation("secured"), "401"); !slices.Equal(got, slices.Insert(slices.Clone(rateLimit), 3, "WWW-Authenticate")) {
		t.Errorf("401 headers %v", got)
	}
	if unauthorized.Description != "Unauthorized" || unauthorized.Headers["WWW-Authenticate"].Description == "" ||
		unauthorized.Headers["RateLimit-Reset"].Schema["type"] != "integer" {
		t.Errorf("401 %+v", unauthorized)
	}

	// Authenticated first, a request rejected with 401 carries no rate limit headers, and the 429 no challenge.
	limited := routeFor[struct{}, struct{}]("limited", "GET", "/limited")
	limited.Middlewares = []forgo.MiddlewareFactory{fhttp.RateLimit(fhttp.RateLimitConfig{DisableHeaders: true})}
	cfg = fhttp.Config{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{auth}},
		Routes: []fhttp.RouteConfig{limited}}
	if doc, err = New(cfg).Generate(context.Background()); err != nil {
		t.Fatal(err)
	}
	op := doc.Operation("limited")
	for status, want := range map[string][]string{
		"200":     {"X-Request-Id"},
		"401":     {"WWW-Authenticate", "X-Request-Id"},
		"429":     {"Retry-After", "X-Request-Id"},
		"default": {"X-Request-Id"},
	} {
		if got := headers(op, status); !slices.Equal(got, want) {
			t.Errorf("%s headers %v, want %v", status, got, want)
		}
	}

	// Statuses come from the serializer: one that answers every error 400 merges the rejections.
	cfg.Serializer = testFactory
	if doc, err = New(cfg).Generate(context.Background()); err != nil {
		t.Fatal(err)
	}
	op = doc.Operation("limited")
	if got := slices.Sorted(maps.Keys(op.Responses)); !slices.Equal(got, []string{"200", "400", "default"}) {
		t.Fatalf("responses %v", got)
	}
	if got := headers(op, "400"); !slices.Equal(got, []string{"Retry-After", "WWW-Authenticate", "X-Request-Id"}) {
		t.Fatalf("400 headers %v", got)
	}

	// A cookie scheme cannot take the name of another scheme.
	clash := routeFor[struct{}, struct{}]("clash", "GET", "/clash")
	clash.Middlewares = []forgo.MiddlewareFactory{fhttp.JWTAuth(fhttp.JWTConfig{Keys: keys, Cookie: "bearerAuth"})}
	_, err = New(fhttp.Config{Routes: []fhttp.RouteConfig{clash}}).Generate(context.Background())
	if err == nil || !strings.Contains(err.Error(), `security scheme "bearerAuth"`) {
		t.Fatalf("clash: %v", err)
	}
}

// TestCustomMiddlewareContract checks what the generator documents from a contract given with http.WithContract,
// whose factory it never calls, and that it fails on contracts it cannot document.
func TestCustomMiddlewareContract(t *testing.T) {
	forbidden := operation.Error{Code: "forbidden", Status: operation.StatusPermissionDenied, Message: "forbidden"}
	route := routeFor[struct{}, struct{}]("custom", "GET", "/custom")
	route.Middlewares = []forgo.MiddlewareFactory{fhttp.WithContract(fhttp.MiddlewareContract{
		Auth: &fhttp.AuthContract{Credentials: []fhttp.Credential{
			{Kind: fhttp.CredentialHeader, Name: "X-API-Key"},
			{Kind: fhttp.CredentialQuery, Name: "api_key"},
			{Kind: fhttp.CredentialBasic},
		}},
		Headers:    []fhttp.ResponseHeader{{Name: "X-Tenant", Description: "The tenant.", Type: reflect.TypeFor[string]()}},
		Rejections: []fhttp.Rejection{{Err: forbidden, Headers: []fhttp.ResponseHeader{{Name: "X-Denied-By"}}}},
	}, nil)}
	doc, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{route}}).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	wantSchemes := map[string]SecurityScheme{
		"X-API-Key": {Type: "apiKey", In: "header", Name: "X-API-Key"},
		"api_key":   {Type: "apiKey", In: "query", Name: "api_key"},
		"basicAuth": {Type: "http", Scheme: "basic"},
	}
	if !maps.Equal(doc.Components.SecuritySchemes, wantSchemes) {
		t.Fatalf("schemes %+v", doc.Components.SecuritySchemes)
	}
	op := doc.Operation("custom")
	if got, want := string(mustJSON(t, op.Security)), `[{"X-API-Key":[]},{"api_key":[]},{"basicAuth":[]}]`; got != want {
		t.Errorf("security %s, want %s", got, want)
	}
	for status, want := range map[string][]string{
		"200":     {"X-Request-Id", "X-Tenant"},
		"403":     {"X-Denied-By", "X-Request-Id", "X-Tenant"},
		"default": {"X-Request-Id", "X-Tenant"},
	} {
		if got := slices.Sorted(maps.Keys(op.Responses[status].Headers)); !slices.Equal(got, want) {
			t.Errorf("%s headers %v, want %v", status, got, want)
		}
	}
	denied := op.Responses["403"]
	if denied.Description != "Forbidden" || denied.Headers["X-Denied-By"].Schema["type"] != "string" ||
		denied.Headers["X-Tenant"].Description != "The tenant." {
		t.Errorf("403 %+v", denied)
	}

	for name, tc := range map[string]struct {
		contract fhttp.MiddlewareContract
		want     string
	}{
		"no credential":   {fhttp.MiddlewareContract{Auth: &fhttp.AuthContract{Optional: true}}, "no credential"},
		"no kind":         {fhttp.MiddlewareContract{Auth: &fhttp.AuthContract{Credentials: []fhttp.Credential{{Name: "key"}}}}, "unknown kind 0"},
		"unnamed key":     {fhttp.MiddlewareContract{Auth: &fhttp.AuthContract{Credentials: []fhttp.Credential{{Kind: fhttp.CredentialCookie}}}}, "cookie credential has no name"},
		"unnamed header":  {fhttp.MiddlewareContract{Headers: []fhttp.ResponseHeader{{Description: "?"}}}, "header without a name"},
		"no error":        {fhttp.MiddlewareContract{Rejections: []fhttp.Rejection{{}}}, "rejection without an error"},
		"unnamed details": {fhttp.MiddlewareContract{Rejections: []fhttp.Rejection{{Err: forbidden, Headers: []fhttp.ResponseHeader{{}}}}}, "header without a name"},
	} {
		route.Middlewares = []forgo.MiddlewareFactory{fhttp.WithContract(tc.contract, nil)}
		_, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{route}}).Generate(context.Background())
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
