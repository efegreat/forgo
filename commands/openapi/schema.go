package openapi

import (
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

var (
	timeType      = reflect.TypeFor[time.Time]()
	numberType    = reflect.TypeFor[json.Number]()
	rawType       = reflect.TypeFor[json.RawMessage]()
	marshalerType = reflect.TypeFor[json.Marshaler]()
	textType      = reflect.TypeFor[encoding.TextMarshaler]()
	safeName      = regexp.MustCompile(`[^a-zA-Z0-9._-]`)
)

type schemaKey struct {
	t       reflect.Type
	request bool
}
type schemaBuilder struct {
	components map[string]Schema
	names      map[schemaKey]string
	building   map[schemaKey]bool
}

func newSchemaBuilder(components map[string]Schema) *schemaBuilder {
	return &schemaBuilder{components: components, names: make(map[schemaKey]string), building: make(map[schemaKey]bool)}
}

func isSpecial(t reflect.Type) bool {
	return t == timeType || t == numberType || t == rawType || t.Implements(marshalerType) || reflect.PointerTo(t).Implements(marshalerType) || t.Implements(textType) || reflect.PointerTo(t).Implements(textType)
}

func nullable(s Schema) Schema { return Schema{"anyOf": []Schema{s, {"type": "null"}}} }

func (b *schemaBuilder) schema(t reflect.Type, request bool) (Schema, error) {
	if t.Kind() == reflect.Pointer {
		s, err := b.schema(t.Elem(), request)
		if err != nil {
			return nil, err
		}
		return nullable(s), nil
	}
	if t == timeType {
		return Schema{"type": "string", "format": "date-time"}, nil
	}
	if t == numberType {
		return Schema{"type": "number"}, nil
	}
	if t == rawType {
		return Schema{}, nil
	}
	if t.Implements(marshalerType) || reflect.PointerTo(t).Implements(marshalerType) {
		return nil, fmt.Errorf("type %s implements json.Marshaler; its JSON shape cannot be inferred", t)
	}
	if t.Implements(textType) || reflect.PointerTo(t).Implements(textType) {
		return Schema{"type": "string"}, nil
	}
	s := Schema{}
	switch t.Kind() {
	case reflect.Bool:
		s["type"] = "boolean"
	case reflect.String:
		s["type"] = "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s["type"] = "integer"
		if t.Kind() == reflect.Int64 {
			s["format"] = "int64"
		} else if t.Kind() == reflect.Int32 {
			s["format"] = "int32"
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		s["type"] = "integer"
		s["minimum"] = uint64(0)
		if t.Kind() == reflect.Uint64 {
			s["format"] = "uint64"
		}
	case reflect.Float32, reflect.Float64:
		s["type"] = "number"
		if t.Kind() == reflect.Float32 {
			s["format"] = "float"
		} else {
			s["format"] = "double"
		}
	case reflect.Slice, reflect.Array:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return Schema{"type": "string", "contentEncoding": "base64"}, nil
		}
		items, err := b.schema(t.Elem(), request)
		if err != nil {
			return nil, err
		}
		s["type"] = "array"
		s["items"] = items
		if t.Kind() == reflect.Array {
			s["minItems"] = t.Len()
			s["maxItems"] = t.Len()
		} else {
			return nullable(s), nil
		}
	case reflect.Map:
		key := t.Key()
		if key.Kind() != reflect.String && !isInteger(key.Kind()) && !key.Implements(textType) {
			return nil, fmt.Errorf("map key type %s is not JSON-compatible", key)
		}
		value, err := b.schema(t.Elem(), request)
		if err != nil {
			return nil, err
		}
		return nullable(Schema{"type": "object", "additionalProperties": value}), nil
	case reflect.Interface:
		return s, nil
	case reflect.Struct:
		key := schemaKey{t, request}
		if name, ok := b.names[key]; ok {
			return Schema{"$ref": "#/components/schemas/" + name}, nil
		}
		if t.Name() == "" {
			if b.building[key] {
				return nil, fmt.Errorf("recursive unnamed type %s", t)
			}
			b.building[key] = true
			defer delete(b.building, key)
			return b.object(t, request)
		}
		prefix := "Response_"
		if request {
			prefix = "Request_"
		}
		hash := sha256.Sum256([]byte(t.PkgPath() + ":" + t.String()))
		name := prefix + safeName.ReplaceAllString(t.Name(), "_") + fmt.Sprintf("_%x", hash[:6])
		b.names[key] = name // register before descending to handle recursive types
		obj, err := b.object(t, request)
		if err != nil {
			return nil, err
		}
		b.components[name] = obj
		return Schema{"$ref": "#/components/schemas/" + name}, nil
	default:
		return nil, fmt.Errorf("unsupported Go type %s", t)
	}
	return s, nil
}

func isInteger(k reflect.Kind) bool { return k >= reflect.Int && k <= reflect.Uintptr }

type jsonField struct {
	field    reflect.StructField
	name     string
	depth    int
	tagged   bool
	optional bool
	quoted   bool
}

// fields implements JSON embedding dominance: shallow fields win, a tagged
// field wins over untagged peers, and equally dominant conflicts disappear.
// In request mode, only the selected json source participates.
func fields(t reflect.Type, request bool) ([]jsonField, error) {
	candidates := make(map[string][]jsonField)
	stack := make(map[reflect.Type]bool)
	var walk func(reflect.Type, int, bool) error
	walk = func(t reflect.Type, depth int, optional bool) error {
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
			tag, has := f.Tag.Lookup("json")
			parts := strings.Split(tag, ",")
			name := parts[0]
			if name == "-" {
				continue
			}
			if name != "" && !validJSONName(name) {
				name = ""
			}
			if request {
				kind, _, err := source(f)
				if err != nil {
					return err
				}
				if kind != "json" {
					if kind == "" && f.Anonymous && ft.Kind() == reflect.Struct {
						if err := walk(ft, depth+1, optional || f.Type.Kind() == reflect.Pointer); err != nil {
							return err
						}
					}
					continue
				}
			}
			if f.Anonymous && name == "" && ft.Kind() == reflect.Struct && !isSpecial(ft) {
				if err := walk(ft, depth+1, optional || f.Type.Kind() == reflect.Pointer); err != nil {
					return err
				}
				continue
			}
			if f.PkgPath != "" {
				continue
			}
			tagged := name != "" && has
			if name == "" {
				name = f.Name
			}
			jf := jsonField{field: f, name: name, depth: depth, tagged: tagged, optional: optional}
			for _, option := range parts[1:] {
				if option == "omitempty" || option == "omitzero" {
					jf.optional = true
				}
				if option == "string" {
					jf.quoted = true
				}
			}
			candidates[name] = append(candidates[name], jf)
		}
		return nil
	}
	if err := walk(t, 0, false); err != nil {
		return nil, err
	}
	var result []jsonField
	for _, group := range candidates {
		sort.Slice(group, func(i, j int) bool { return group[i].depth < group[j].depth })
		depth := group[0].depth
		var top []jsonField
		for _, f := range group {
			if f.depth == depth {
				top = append(top, f)
			}
		}
		var tagged []jsonField
		for _, f := range top {
			if f.tagged {
				tagged = append(tagged, f)
			}
		}
		if len(tagged) > 0 {
			top = tagged
		}
		if len(top) == 1 {
			result = append(result, top[0])
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].name < result[j].name })
	return result, nil
}

func validJSONName(s string) bool {
	for _, r := range s {
		if !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", r) && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func (b *schemaBuilder) object(t reflect.Type, request bool) (Schema, error) {
	fs, err := fields(t, request)
	if err != nil {
		return nil, err
	}
	props := make(map[string]Schema)
	required := []string{}
	for _, f := range fs {
		s, err := b.schema(f.field.Type, request)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.field.Name, err)
		}
		need := !f.optional
		if request {
			s, need, err = b.validation(s, f.field.Type, f.field.Tag.Get("validate"))
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", f.field.Name, err)
			}
		}
		if f.quoted {
			k := indirect(f.field.Type).Kind()
			if k == reflect.Bool || k == reflect.String || isInteger(k) || k == reflect.Float32 || k == reflect.Float64 {
				s = Schema{"type": "string"}
				if f.field.Type.Kind() == reflect.Pointer {
					s = nullable(s)
				}
			}
		}
		props[f.name] = s
		if need {
			required = append(required, f.name)
		}
	}
	obj := Schema{"type": "object", "properties": props}
	if len(required) > 0 {
		obj["required"] = required
	}
	return obj, nil
}
