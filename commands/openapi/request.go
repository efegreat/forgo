package openapi

import (
	"fmt"
	"net/textproto"
	"reflect"
	"slices"
	"sort"
	"strings"
)

// fieldSource is one source a request field is bound from: kind is "json", "query", "uri" or "header".
type fieldSource struct {
	kind string
	name string
}

// sourceKinds are the sources in the order the default binder (bingo) applies them. A later source that a request
// sends overwrites an earlier one, so the last one wins.
var sourceKinds = []string{"json", "query", "uri", "header"}

// sources returns the sources the binder binds f from, in sourceKinds order. "-" disables a source. A field
// without a json tag is left out of the body, although encoding/json would decode it by its Go name.
func sources(f reflect.StructField) ([]fieldSource, error) {
	var srcs []fieldSource
	for _, kind := range sourceKinds {
		tag, ok := f.Tag.Lookup(kind)
		if !ok {
			continue
		}
		name := strings.SplitN(tag, ",", 2)[0]
		if tag == "-" || (name == "-" && kind != "json") {
			continue // encoding/json reads "-," as the name "-"
		}
		if name == "" {
			if kind != "json" {
				return nil, fmt.Errorf("field %s: empty %s tag", f.Name, kind)
			}
			name = f.Name
		}
		if kind == "json" && !validJSONName(name) {
			name = f.Name
		}
		if kind == "header" {
			name = textproto.CanonicalMIMEHeaderKey(name)
		}
		srcs = append(srcs, fieldSource{kind: kind, name: name})
	}
	return srcs, nil
}

// describe names the source in a sentence.
func (s fieldSource) describe() string {
	switch s.kind {
	case "json":
		return fmt.Sprintf("the body property %q", s.name)
	case "uri":
		return fmt.Sprintf("the path parameter %q", s.name)
	case "query":
		return fmt.Sprintf("the query parameter %q", s.name)
	default:
		return fmt.Sprintf("the header %q", s.name)
	}
}

// sourceNote tells the reader of srcs[self] about the other sources of a field bound from several, and which one
// wins when a request sends more than one. It is empty for a field with one source.
func sourceNote(srcs []fieldSource, self int) string {
	if len(srcs) < 2 {
		return ""
	}
	var others []string
	for i, src := range srcs {
		if i != self {
			others = append(others, src.describe())
		}
	}
	winner := "this one"
	if last := len(srcs) - 1; self != last {
		winner = srcs[last].describe()
	}
	return fmt.Sprintf("Also accepted as %s. When a request sends several, %s wins.", strings.Join(others, " and "), winner)
}

// joinNotes joins the non-empty sentences of a description.
func joinNotes(notes ...string) string {
	return strings.Join(slices.DeleteFunc(notes, func(s string) bool { return s == "" }), " ")
}

func indirect(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// inspectRequest documents a handler request: its path, header and query fields as parameters and its json
// fields as the body. vars maps the wildcards of the route path to whether they match the rest of the path. The
// path parameters the request binds are added to covered.
func inspectRequest(b *schemaBuilder, t reflect.Type, vars, covered map[string]bool) ([]Parameter, *RequestBody, error) {
	if t == nil {
		return nil, nil, nil
	}
	t = indirect(t)
	if t.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("request type must be a struct, got %s", t)
	}
	params, err := inspectParams(b, t, vars, covered)
	if err != nil {
		return nil, nil, err
	}
	body, err := inspectBody(b, t)
	if err != nil {
		return nil, nil, err
	}
	return params, body, nil
}

// inspectMiddleware documents the Params of a middleware created by http.MiddlewareAs. They come from the path,
// headers and query only, because the body belongs to the handler. The path parameters they declare are added to
// covered.
func inspectMiddleware(b *schemaBuilder, t reflect.Type, vars, covered map[string]bool) ([]Parameter, error) {
	t = indirect(t)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("params type must be a struct, got %s", t)
	}
	params, err := inspectParams(b, t, vars, covered)
	if err != nil {
		return nil, err
	}
	body, err := inspectBody(b, t)
	if err != nil {
		return nil, err
	}
	if body != nil {
		return nil, fmt.Errorf("json fields are not supported, because the request body belongs to the handler")
	}
	return params, nil
}

// inspectParams documents the path, header and query fields of the struct t, including those of nested value
// structs. A field bound from several sources is documented in each, and required in none but the path. The path
// parameters among them are added to covered.
func inspectParams(b *schemaBuilder, t reflect.Type, vars, covered map[string]bool) ([]Parameter, error) {
	var params []Parameter
	seen := make(map[string]bool)
	stack := make(map[reflect.Type]bool)
	var walk func(reflect.Type) error
	walk = func(t reflect.Type) error {
		if stack[t] {
			return nil
		}
		stack[t] = true
		defer delete(stack, t)
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			ft := indirect(f.Type)
			if f.PkgPath != "" && !(f.Anonymous && ft.Kind() == reflect.Struct) {
				continue
			}
			srcs, err := sources(f)
			if err != nil {
				return err
			}
			textual := false
			for j, src := range srcs {
				if src.kind == "json" {
					continue
				}
				textual = true
				p, err := param(b, f, src, vars, covered)
				if err != nil {
					return err
				}
				key := p.In + ":" + p.Name
				if seen[key] {
					return fmt.Errorf("duplicate %s parameter %q", p.In, p.Name)
				}
				seen[key] = true
				p.Description = joinNotes(p.Description, sourceNote(srcs, j))
				if len(srcs) > 1 && p.In != "path" {
					p.Required = false // any of the sources satisfies the validation
				}
				params = append(params, p)
			}
			if !textual && ft.Kind() == reflect.Struct && !b.leaf(ft, true) {
				if err := walk(ft); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(t); err != nil {
		return nil, err
	}
	return params, nil
}

// param documents the field f as the parameter of its textual source src.
func param(b *schemaBuilder, f reflect.StructField, src fieldSource, vars, covered map[string]bool) (Parameter, error) {
	p := Parameter{Name: src.name, In: src.kind}
	if p.In == "uri" {
		p.In = "path"
		remainder, ok := vars[p.Name]
		if !ok {
			return Parameter{}, fmt.Errorf("uri parameter %q does not exist in route path", p.Name)
		}
		covered[p.Name] = true
		if remainder {
			p.Description = "Matches the rest of the path, slashes included."
		}
	}
	s, repeated, err := b.paramSchema(f.Type)
	if err != nil {
		return Parameter{}, fmt.Errorf("parameter %q: %w", p.Name, err)
	}
	if repeated {
		switch p.In {
		case "path":
			// The binder binds a wildcard as one value, without splitting it.
			return Parameter{}, fmt.Errorf("parameter %q: a path wildcard holds one value, so a uri field cannot be a slice or an array", p.Name)
		case "header":
			// The binder takes one value per header line and does not split them at commas.
			p.Description = "Send one header line per value."
		}
	}
	s, required, err := b.validation(s, f.Type, f.Tag.Get("validate"), false)
	if err != nil {
		return Parameter{}, fmt.Errorf("parameter %q: %w", p.Name, err)
	}
	p.Schema, p.Required = s, p.In == "path" || required
	if p.In == "query" {
		explode := true
		p.Style, p.Explode = "form", &explode
	}
	return p, nil
}

// inspectBody documents the json fields of the struct t as the request body, or returns nil when it has none.
func inspectBody(b *schemaBuilder, t reflect.Type) (*RequestBody, error) {
	body, err := b.object(t, true)
	if err != nil {
		return nil, err
	}
	if len(body["properties"].(map[string]Schema)) == 0 {
		return nil, nil
	}
	required, _ := body["required"].([]string)
	return &RequestBody{Required: len(required) > 0, Content: map[string]MediaType{"application/json": {Schema: body}}}, nil
}

// mergeParams adds the parameters of a middleware to those documented so far. A parameter declared twice is
// documented once, as required when either declaration requires it, and both must have the same schema.
func mergeParams(params, more []Parameter) ([]Parameter, error) {
	for _, p := range more {
		i := slices.IndexFunc(params, func(q Parameter) bool { return q.In == p.In && q.Name == p.Name })
		if i < 0 {
			params = append(params, p)
			continue
		}
		q := &params[i]
		if !reflect.DeepEqual(q.Schema, p.Schema) || q.Style != p.Style || !reflect.DeepEqual(q.Explode, p.Explode) {
			return nil, fmt.Errorf("%s parameter %q is declared with a different schema elsewhere on the route", p.In, p.Name)
		}
		q.Required = q.Required || p.Required
		if q.Description == "" {
			q.Description = p.Description
		}
	}
	return params, nil
}

func sortParams(params []Parameter) {
	sort.Slice(params, func(i, j int) bool {
		if params[i].In != params[j].In {
			return params[i].In < params[j].In
		}
		return params[i].Name < params[j].Name
	})
}
