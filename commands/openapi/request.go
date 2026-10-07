package openapi

import (
	"fmt"
	"net/textproto"
	"reflect"
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

func inspectRequest(b *schemaBuilder, t reflect.Type, vars map[string]bool) ([]Parameter, *RequestBody, error) {
	var params []Parameter
	seen := make(map[string]bool)
	pathSeen := make(map[string]bool)
	if t != nil {
		t = indirect(t)
		if t.Kind() != reflect.Struct {
			return nil, nil, fmt.Errorf("request type must be a struct, got %s", t)
		}
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
						pathSeen[name] = true
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
			return nil, nil, err
		}
	}
	for name := range vars {
		if !pathSeen[name] {
			return nil, nil, fmt.Errorf("path parameter %q has no matching uri tag", name)
		}
	}
	sort.Slice(params, func(i, j int) bool {
		if params[i].In != params[j].In {
			return params[i].In < params[j].In
		}
		return params[i].Name < params[j].Name
	})
	if t == nil {
		return params, nil, nil
	}
	body, err := b.object(t, true)
	if err != nil {
		return nil, nil, err
	}
	if len(body["properties"].(map[string]Schema)) == 0 {
		return params, nil, nil
	}
	required, _ := body["required"].([]string)
	return params, &RequestBody{Required: len(required) > 0, Content: map[string]MediaType{"application/json": {Schema: body}}}, nil
}
