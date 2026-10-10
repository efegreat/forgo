package openapi

import (
	"fmt"
	"net/textproto"
	"reflect"
	"slices"
	"sort"
	"strings"
)

// source selects one enabled source, independently of the configured binder.
func source(f reflect.StructField) (kind, name string, err error) {
	for _, kind = range []string{"uri", "header", "query", "json"} {
		tag, ok := f.Tag.Lookup(kind)
		if !ok {
			continue
		}
		name = strings.SplitN(tag, ",", 2)[0]
		if name == "-" {
			continue
		}
		if name == "" {
			if kind != "json" {
				return "", "", fmt.Errorf("field %s: empty %s tag", f.Name, kind)
			}
			name = f.Name
		}
		return kind, name, nil
	}
	return "", "", nil
}

func indirect(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// inspectRequest documents a handler request: its path, header and query fields as parameters and its json
// fields as the body. The path parameters it binds are added to covered.
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

// inspectParams documents the path, header and query fields of the struct t. The path parameters among them are
// added to covered.
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
			if f.PkgPath != "" && !(f.Anonymous && indirect(f.Type).Kind() == reflect.Struct) {
				continue
			}
			kind, name, err := source(f)
			if err != nil {
				return err
			}
			if kind != "" && kind != "json" {
				in := kind
				if in == "uri" {
					in = "path"
				}
				if in == "header" {
					name = textproto.CanonicalMIMEHeaderKey(name)
				}
				key := in + ":" + name
				if seen[key] {
					return fmt.Errorf("duplicate %s parameter %q", in, name)
				}
				seen[key] = true
				if in == "path" {
					if !vars[name] {
						return fmt.Errorf("uri parameter %q does not exist in route path", name)
					}
					covered[name] = true
				}
				ft := indirect(f.Type)
				if (ft.Kind() == reflect.Struct && !isSpecial(ft)) || ft.Kind() == reflect.Map || ft.Kind() == reflect.Interface {
					return fmt.Errorf("parameter %q: unsupported textual type %s", name, f.Type)
				}
				s, err := b.schema(f.Type, true)
				// JSON byte slices are base64 strings, but textual-source byte
				// slices follow the ordinary repeated-value array convention.
				if ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Uint8 {
					s = Schema{"type": "array", "items": Schema{"type": "integer", "minimum": 0}}
				}
				if err != nil {
					return fmt.Errorf("parameter %q: %w", name, err)
				}
				s, required, err := b.validation(s, f.Type, f.Tag.Get("validate"))
				if err != nil {
					return fmt.Errorf("parameter %q: %w", name, err)
				}
				p := Parameter{Name: name, In: in, Required: in == "path" || required, Schema: s}
				if ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array {
					et := indirect(ft.Elem())
					if (et.Kind() == reflect.Struct && !isSpecial(et)) || et.Kind() == reflect.Map || et.Kind() == reflect.Slice || et.Kind() == reflect.Array {
						return fmt.Errorf("parameter %q: nested textual collection is unsupported", name)
					}
				}
				if in == "query" {
					explode := true
					p.Style = "form"
					p.Explode = &explode
				}
				params = append(params, p)
				continue
			}
			ft := indirect(f.Type)
			if ft.Kind() == reflect.Struct && !isSpecial(ft) {
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
