package openapi

import (
	"context"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	fhttp "github.com/efegreat/forgo/server/http"
)

func TestPathTemplate(t *testing.T) {
	for _, tc := range []struct {
		path     string
		template string
		vars     map[string]bool
		err      string
	}{
		{path: "/users/{id}/posts/{post}", template: "/users/{id}/posts/{post}", vars: map[string]bool{"id": false, "post": false}},
		{path: "/files/{rest...}", template: "/files/{rest}", vars: map[string]bool{"rest": true}},
		{path: "/{$}", template: "/", vars: map[string]bool{}},
		{path: "/users/{$}", template: "/users/", vars: map[string]bool{}},
		{path: "/users/", template: "/users/", vars: map[string]bool{}},
		{path: "/r/{id:[0-9]+}", err: `bad wildcard name "id:[0-9]+"`},
		{path: "/p/{id}.json", err: "bad wildcard segment"},
		{path: "/p/x{id}", err: "bad wildcard segment"},
		{path: "/a/{", err: "bad wildcard segment"},
		{path: "/a/{x}/{x}", err: `duplicate wildcard name "x"`},
		{path: "/a/{rest...}/b", err: "{...} wildcard not at end"},
		{path: "/a/{$}/b", err: "{$} not at end"},
		{path: "/a/{1x}", err: "bad wildcard name"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			template, vars, err := pathTemplate("GET", tc.path)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) || !strings.Contains(err.Error(), "invalid ServeMux pattern") {
					t.Fatalf("got %q, %v, want error %q", template, err, tc.err)
				}
				return
			}
			if err != nil || template != tc.template || !maps.Equal(vars, tc.vars) {
				t.Fatalf("got %q %v %v, want %q %v", template, vars, err, tc.template, tc.vars)
			}
		})
	}
}

func TestRouteMethods(t *testing.T) {
	for _, tc := range []struct{ method, err string }{
		{"", "an empty Method matches every method"},
		{"get", `method "get" never matches GET requests, because ServeMux methods are case-sensitive: use "GET"`},
		{"CONNECT", `OpenAPI 3.1 cannot describe method "CONNECT": set DisableAPIDoc`},
		{"PURGE", `cannot describe method "PURGE"`},
	} {
		t.Run(tc.method, func(t *testing.T) {
			route := fhttp.RouteConfig{Method: tc.method, Path: "/any", Handler: fhttp.EmptyHandler()}
			_, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{route}}).Generate(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("got %v, want %q", err, tc.err)
			}
		})
	}
	for _, method := range operationMethods {
		route := fhttp.RouteConfig{Method: method, Path: "/any", Handler: fhttp.EmptyHandler()}
		doc, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{route}}).Generate(context.Background())
		if err != nil || doc.Paths["/any"][strings.ToLower(method)] == nil {
			t.Fatalf("%s: %v", method, err)
		}
	}
}

func TestRouteLabels(t *testing.T) {
	route := routeFor[struct {
		ID string `uri:"missing"`
	}, struct{}]("", "GET", "/items/{id}")
	route.Version = "v1"
	_, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{route}}).Generate(context.Background())
	if want := `openapi: route GET /v1/items/{id}: uri parameter "missing" does not exist in route path`; err == nil || err.Error() != want {
		t.Fatalf("got %v, want %q", err, want)
	}
	route.Name = "getItem"
	_, err = New(fhttp.Config{Routes: []fhttp.RouteConfig{route}}).Generate(context.Background())
	if want := `openapi: route "getItem" (GET /v1/items/{id}):`; err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("got %v, want prefix %q", err, want)
	}
}

type remainderRequest struct {
	Path string `uri:"path"`
}

func TestRemainderAndExactPaths(t *testing.T) {
	cfg := fhttp.Config{Routes: []fhttp.RouteConfig{
		routeFor[remainderRequest, struct{}]("files", "GET", "/files/{path...}"),
		{Name: "home", Method: "GET", Path: "/{$}", Handler: fhttp.EmptyHandler()},
	}}
	doc, err := New(cfg).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p := paramsByName(doc.Paths["/files/{path}"]["get"])["path:path"]
	if !p.Required || p.Description != "Matches the rest of the path, slashes included." {
		t.Fatalf("remainder parameter %+v", p)
	}
	if doc.Paths["/"]["get"] == nil {
		t.Fatalf("exact match path: %v", doc.Paths)
	}
}

// textID is decoded from text only, as bingo binds it.
type textID struct{ value uint64 }

func (id *textID) UnmarshalText(b []byte) (err error) {
	id.value, err = strconv.ParseUint(string(b), 10, 64)
	return err
}

type paramsRequest struct {
	ID      textID     `uri:"id"`
	Limit   *int       `query:"limit" validate:"required,max=100"`
	Tags    []string   `query:"tag"`
	Since   *time.Time `query:"since"`
	IDs     []uint64   `header:"X-Ids"`
	Weights [2]float32 `query:"w"`
	Raw     []byte     `query:"raw"`
}

func TestParameterSchemas(t *testing.T) {
	doc, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{
		routeFor[paramsRequest, struct{}]("list", "GET", "/items/{id}"),
	}}).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	params := paramsByName(doc.Paths["/items/{id}"]["get"])
	for _, tc := range []struct {
		key, schema, description string
		required                 bool
	}{
		{"path:id", `{"type":"string"}`, "", true},
		// A parameter is never null: a pointer only makes it optional, and required adds no "not null".
		{"query:limit", `{"allOf":[{"type":"integer"},{"maximum":100}]}`, "", true},
		{"query:tag", `{"items":{"type":"string"},"type":"array"}`, "", false},
		{"query:since", `{"format":"date-time","type":"string"}`, "", false},
		{"header:X-Ids", `{"items":{"format":"uint64","minimum":0,"type":"integer"},"type":"array"}`, "Send one header line per value.", false},
		{"query:w", `{"items":{"format":"float","type":"number"},"maxItems":2,"minItems":2,"type":"array"}`, "", false},
		{"query:raw", `{"items":{"minimum":0,"type":"integer"},"type":"array"}`, "", false},
	} {
		p, ok := params[tc.key]
		if !ok {
			t.Fatalf("%s missing from %v", tc.key, params)
		}
		if got := string(mustJSON(t, p.Schema)); got != tc.schema || p.Description != tc.description || p.Required != tc.required {
			t.Fatalf("%s: schema %s, description %q, required %v", tc.key, got, p.Description, p.Required)
		}
	}
	if q := params["query:tag"]; q.Style != "form" || q.Explode == nil || !*q.Explode {
		t.Fatalf("query style %+v", q)
	}
}

func TestParameterFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		err  string
	}{
		{"path slice", reflect.TypeFor[struct {
			IDs []string `uri:"id"`
		}](), `parameter "id": a path wildcard holds one value, so a uri field cannot be a slice or an array`},
		{"map", reflect.TypeFor[struct {
			Meta map[string]string `query:"meta"`
		}](), `parameter "meta": unsupported textual type map[string]string`},
		{"struct", reflect.TypeFor[struct {
			Page struct{ N int } `query:"page"`
		}](), `parameter "page": unsupported textual type struct { N int }`},
		{"nested collection", reflect.TypeFor[struct {
			Groups [][]int `query:"g"`
		}](), "a repeated parameter holds text values only"},
		{"empty tag", reflect.TypeFor[struct {
			Page int `query:""`
		}](), "field Page: empty query tag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := inspectRequest(newSchemaBuilder(nil), tc.typ, map[string]bool{"id": false}, map[string]bool{})
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("got %v, want %q", err, tc.err)
			}
		})
	}
}

type multiSourceRequest struct {
	Locale string `header:"Accept-Language" query:"locale" json:"locale" validate:"required"`
	ID     string `uri:"id" json:"id" validate:"required"`
	Name   string `json:"name" validate:"required"`
}

func TestMultiSourceFields(t *testing.T) {
	doc, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{
		routeFor[multiSourceRequest, struct{}]("update", "PUT", "/items/{id}"),
	}}).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	op := doc.Paths["/items/{id}"]["put"]
	params := paramsByName(op)
	header, query, path := params["header:Accept-Language"], params["query:locale"], params["path:id"]
	if header.Required || query.Required || !path.Required {
		t.Fatalf("required: header %v, query %v, path %v", header.Required, query.Required, path.Required)
	}
	if want := `Also accepted as the body property "locale" and the query parameter "locale". When a request sends several, this one wins.`; header.Description != want {
		t.Fatalf("header note %q", header.Description)
	}
	if want := `Also accepted as the body property "locale" and the header "Accept-Language". When a request sends several, the header "Accept-Language" wins.`; query.Description != want {
		t.Fatalf("query note %q", query.Description)
	}
	body := op.RequestBody.Content["application/json"].Schema
	props := body["properties"].(map[string]Schema)
	if want := `Also accepted as the query parameter "locale" and the header "Accept-Language". When a request sends several, the header "Accept-Language" wins.`; props["locale"]["description"] != want {
		t.Fatalf("body note %q", props["locale"]["description"])
	}
	if want := `Also accepted as the path parameter "id". When a request sends several, the path parameter "id" wins.`; props["id"]["description"] != want {
		t.Fatalf("body id note %q", props["id"]["description"])
	}
	// Only the field bound from the body alone is required there.
	if required := body["required"].([]string); len(required) != 1 || required[0] != "name" {
		t.Fatalf("body required %v", required)
	}
}
