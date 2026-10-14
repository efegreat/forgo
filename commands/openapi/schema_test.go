package openapi

import (
	"context"
	"encoding/json"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"

	fhttp "github.com/efegreat/forgo/server/http"
	"github.com/efegreat/forgo/types"

	"github.com/google/uuid"
)

// money has its own JSON encoding, like decimal types do.
//
//lint:ignore U1000 the field makes money a struct, which its MarshalJSON hides from the schema.
type money struct{ cents int64 }

func (m money) MarshalJSON() ([]byte, error)     { return []byte(`"1.00"`), nil }
func (m *money) UnmarshalJSON(data []byte) error { return nil }

type orderStatus string

type orderRequest struct {
	Total  money       `json:"total" validate:"required"`
	Tip    *money      `json:"tip"`
	Status orderStatus `json:"status"`
	Max    money       `query:"max"`
}

type orderResponse struct {
	Total money `json:"total"`
}

func TestWithSchema(t *testing.T) {
	route := routeFor[orderRequest, orderResponse]("order", "POST", "/orders")
	cfg := fhttp.Config{Routes: []fhttp.RouteConfig{route}}
	if _, err := New(cfg).Generate(context.Background()); err == nil || !strings.Contains(err.Error(), `parameter "max": unsupported textual type openapi.money`) {
		t.Fatalf("parameter without WithSchema: %v", err)
	}
	if _, err := newSchemaBuilder(nil).schema(reflect.TypeFor[money](), true); err == nil ||
		err.Error() != "type openapi.money implements json.Unmarshaler, so its JSON shape cannot be inferred: describe it with WithSchema" {
		t.Fatalf("body without WithSchema: %v", err)
	}

	decimal := Schema{"type": "string", "format": "decimal"}
	doc, err := New(cfg,
		WithSchema[money](decimal),
		WithSchema[orderStatus](Schema{"type": "string", "enum": []string{"open", "paid"}}),
	).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	op := doc.Paths["/orders"]["post"]
	props := op.RequestBody.Content["application/json"].Schema["properties"].(map[string]Schema)
	for name, want := range map[string]string{
		"total":  `{"format":"decimal","type":"string"}`,
		"tip":    `{"anyOf":[{"format":"decimal","type":"string"},{"type":"null"}]}`,
		"status": `{"enum":["open","paid"],"type":"string"}`,
	} {
		if got := string(mustJSON(t, props[name])); got != want {
			t.Fatalf("body %s: %s, want %s", name, got, want)
		}
	}
	if got := string(mustJSON(t, paramsByName(op)["query:max"].Schema)); got != `{"format":"decimal","type":"string"}` {
		t.Fatalf("parameter: %s", got)
	}
	result := op.Responses["200"].Content["application/json"].Schema["properties"].(map[string]Schema)["result"]
	component := doc.Components.Schemas[strings.TrimPrefix(result["$ref"].(string), componentRef)]
	if got := string(mustJSON(t, component["properties"].(map[string]Schema)["total"])); got != `{"format":"decimal","type":"string"}` {
		t.Fatalf("response: %s", got)
	}
	// The document gets copies: changing it leaves the option's schema alone.
	props["total"]["format"] = "changed"
	if decimal["format"] != "decimal" {
		t.Fatal("document shares the WithSchema schema")
	}
}

type textOnly struct{ v string }

func (t *textOnly) UnmarshalText(b []byte) error { t.v = string(b); return nil }

type jsonOnly struct{ V string }

func (j *jsonOnly) UnmarshalJSON([]byte) error { return nil }

type textOut struct{ v string }

func (t textOut) MarshalText() ([]byte, error) { return []byte(t.v), nil }

func TestCodecDirections(t *testing.T) {
	b := newSchemaBuilder(nil)
	// Request bodies are decoded, so the unmarshaling interfaces decide their shape.
	if s, err := b.schema(reflect.TypeFor[textOnly](), true); err != nil || s["type"] != "string" {
		t.Fatalf("request TextUnmarshaler: %v %v", s, err)
	}
	if _, err := b.schema(reflect.TypeFor[jsonOnly](), true); err == nil || !strings.Contains(err.Error(), "implements json.Unmarshaler") {
		t.Fatalf("request json.Unmarshaler: %v", err)
	}
	// Responses are encoded: a type that only decodes itself is documented by its fields.
	if s, err := b.schema(reflect.TypeFor[jsonOnly](), false); err != nil || s["$ref"] == nil {
		t.Fatalf("response of a decoder: %v %v", s, err)
	}
	if s, err := b.schema(reflect.TypeFor[textOut](), false); err != nil || s["type"] != "string" {
		t.Fatalf("response TextMarshaler: %v %v", s, err)
	}
	if _, err := b.schema(reflect.TypeFor[money](), false); err == nil || !strings.Contains(err.Error(), "implements json.Marshaler") {
		t.Fatalf("response json.Marshaler: %v", err)
	}
	// uuid.UUID is a byte array that encodes and decodes itself as text.
	for _, request := range []bool{true, false} {
		if s, err := b.schema(reflect.TypeFor[uuid.UUID](), request); err != nil || s["type"] != "string" {
			t.Fatalf("uuid.UUID (request %v): %v %v", request, s, err)
		}
	}
	// types.JSONMap has its own JSON encoding, which is known to be a map[string]V.
	for _, request := range []bool{true, false} {
		s, err := b.schema(reflect.TypeFor[*types.JSONMap[int]](), request)
		want := `{"anyOf":[{"additionalProperties":{"type":"integer"},"type":"object"},{"type":"null"}]}`
		if got := string(mustJSON(t, s)); err != nil || got != want {
			t.Fatalf("*types.JSONMap[int] (request %v): %s %v, want %s", request, got, err, want)
		}
	}
	if s, ok := b.paramValue(reflect.TypeFor[*uuid.UUID]()); !ok || s["type"] != "string" {
		t.Fatalf("uuid.UUID parameter: %v", s)
	}
	// Map keys follow the same direction.
	if _, err := b.schema(reflect.TypeFor[map[textOnly]int](), true); err != nil {
		t.Fatalf("request TextUnmarshaler key: %v", err)
	}
	if _, err := b.schema(reflect.TypeFor[map[textOnly]int](), false); err == nil {
		t.Fatal("response key without TextMarshaler accepted")
	}
}

type recursiveUnnamed struct {
	Next *struct {
		Value int `json:"value"`
	} `json:"next"`
}

func TestSchemaTypes(t *testing.T) {
	b := newSchemaBuilder(nil)
	for _, tc := range []struct {
		t    reflect.Type
		want string
	}{
		{reflect.TypeFor[json.RawMessage](), `{}`},
		{reflect.TypeFor[any](), `{}`},
		{reflect.TypeFor[json.Number](), `{"type":"number"}`},
		{reflect.TypeFor[textOut](), `{"type":"string"}`},
		{reflect.TypeFor[map[int]string](), `{"anyOf":[{"additionalProperties":{"type":"string"},"type":"object"},{"type":"null"}]}`},
		{reflect.TypeFor[map[textOut]bool](), `{"anyOf":[{"additionalProperties":{"type":"boolean"},"type":"object"},{"type":"null"}]}`},
		{reflect.TypeFor[[]int8](), `{"anyOf":[{"items":{"type":"integer"},"type":"array"},{"type":"null"}]}`},
		{reflect.TypeFor[recursiveUnnamed](), `{"$ref":"#/components/schemas/@0"}`},
		{reflect.TypeFor[types.JSONMap[any]](), `{"additionalProperties":{},"type":"object"}`},
		{reflect.TypeFor[types.JSONMap[[]string]](), `{"additionalProperties":{"anyOf":[{"items":{"type":"string"},"type":"array"},{"type":"null"}]},"type":"object"}`},
	} {
		s, err := b.schema(tc.t, false)
		if got := string(mustJSON(t, s)); err != nil || got != tc.want {
			t.Fatalf("%s: %s %v, want %s", tc.t, got, err, tc.want)
		}
	}
	for _, tc := range []struct {
		t   reflect.Type
		err string
	}{
		{reflect.TypeFor[map[struct{ A int }]int](), "map key type struct { A int } is not JSON-compatible"},
		{reflect.TypeFor[func()](), "unsupported Go type func()"},
		{reflect.TypeFor[struct {
			Next []struct{ Self any } `json:"next"`
		}](), ""},
	} {
		_, err := b.schema(tc.t, false)
		if tc.err == "" {
			if err != nil {
				t.Fatalf("%s: %v", tc.t, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Fatalf("%s: %v, want %q", tc.t, err, tc.err)
		}
	}
}

type promoted struct {
	Inner string `json:"inner"`
}

type hidden struct {
	Promoted string `json:"promoted"`
}

type fieldOptions struct {
	Count   int    `json:"count,string"`
	Maybe   *bool  `json:"maybe,string"`
	Zero    int    `json:"zero,omitzero"`
	Empty   string `json:"empty,omitempty"`
	Bad     string `json:"bad\x01name"`
	Kept    string `json:"-,"` //lint:ignore SA5008 encoding/json names it "-", which the test covers.
	Skipped string `json:"-"`
	*promoted
	hidden
}

func TestFieldOptions(t *testing.T) {
	s, err := newSchemaBuilder(nil).object(reflect.TypeFor[fieldOptions](), false)
	if err != nil {
		t.Fatal(err)
	}
	props := s["properties"].(map[string]Schema)
	want := []string{"-", "Bad", "count", "empty", "inner", "maybe", "promoted", "zero"}
	if got := slices.Sorted(func(yield func(string) bool) {
		for k := range props {
			if !yield(k) {
				return
			}
		}
	}); !slices.Equal(got, want) {
		t.Fatalf("properties %v, want %v", got, want)
	}
	if props["count"]["type"] != "string" || string(mustJSON(t, props["maybe"])) != `{"anyOf":[{"type":"string"},{"type":"null"}]}` {
		t.Fatalf("string option: %v %v", props["count"], props["maybe"])
	}
	// omitempty, omitzero and fields promoted through a nil-able embedded pointer may be missing.
	if required := s["required"].([]string); !slices.Equal(required, []string{"-", "Bad", "count", "maybe", "promoted"}) {
		t.Fatalf("required %v", required)
	}
}

type pageOf[T any] struct {
	Items []T `json:"items"`
}

type product struct {
	ID string `json:"id"`
}

// pageOfProduct has the component name of pageOf[product].
type pageOfProduct struct {
	Total int `json:"total"`
}

// Userinfo has the name of url.Userinfo.
type Userinfo struct {
	Name string `json:"name"`
}

type namesResponse struct {
	Page    pageOf[product] `json:"page"`
	Other   pageOfProduct   `json:"other"`
	Product product         `json:"product"`
	Local   Userinfo        `json:"local"`
	URL     *url.Userinfo   `json:"url"`
}

type namesRequest struct {
	Product product `json:"product"`
	Only    hidden  `json:"only"`
}

func TestComponentNames(t *testing.T) {
	doc, err := New(fhttp.Config{Routes: []fhttp.RouteConfig{
		routeFor[namesRequest, namesResponse]("names", "POST", "/names"),
	}}).Generate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	names := slices.Sorted(func(yield func(string) bool) {
		for k := range doc.Components.Schemas {
			if !yield(k) {
				return
			}
		}
	})
	want := []string{
		"OpenapiUserinfo", // the package name tells two Userinfo types apart
		"UrlUserinfo",     //
		"hidden",          // only in a request: no suffix
		"namesResponse",   //
		// The same name in the same package: hashed.
		"pageOfProduct_" + typeHash(schemaKey{reflect.TypeFor[pageOfProduct](), false}),
		"pageOfProduct_" + typeHash(schemaKey{reflect.TypeFor[pageOf[product]](), false}),
		"product",        //
		"productRequest", // in both a request and a response
	}
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("components %v, want %v", names, want)
	}
	// Every reference points to a component.
	data := string(mustJSON(t, doc))
	for _, part := range strings.Split(data, `"$ref":"`)[1:] {
		name := strings.TrimPrefix(part[:strings.IndexByte(part, '"')], componentRef)
		if doc.Components.Schemas[name] == nil {
			t.Fatalf("dangling reference %q", name)
		}
	}
}

func TestComponentNamesIgnoreRouteOrder(t *testing.T) {
	a := routeFor[struct{}, pageOf[product]]("a", "GET", "/a")
	b := routeFor[struct{}, pageOfProduct]("b", "GET", "/b")
	var previous []string
	for _, routes := range [][]fhttp.RouteConfig{{a, b}, {b, a}} {
		doc, err := New(fhttp.Config{Routes: routes}).Generate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for name := range doc.Components.Schemas {
			names = append(names, name)
		}
		slices.Sort(names)
		if previous != nil && !slices.Equal(names, previous) {
			t.Fatalf("names %v after %v", names, previous)
		}
		previous = names
	}
}

func TestTypeName(t *testing.T) {
	for _, tc := range []struct {
		t    reflect.Type
		want string
	}{
		{reflect.TypeFor[product](), "product"},
		{reflect.TypeFor[pageOf[product]](), "pageOfProduct"},
		{reflect.TypeFor[pageOf[*url.URL]](), "pageOfURL"},
		{reflect.TypeFor[pageOf[map[string][]int]](), "pageOfMapStringInt"},
		{reflect.TypeFor[pageOf[pageOf[product]]](), "pageOfPageOfProduct"},
	} {
		if got := typeName(tc.t); got != tc.want {
			t.Fatalf("%s: %q, want %q", tc.t, got, tc.want)
		}
	}
}
