package openapi

import (
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/efegreat/forgo/safemap"
)

const componentRef = "#/components/schemas/"

var (
	timeType            = reflect.TypeFor[time.Time]()
	numberType          = reflect.TypeFor[json.Number]()
	rawType             = reflect.TypeFor[json.RawMessage]()
	marshalerType       = reflect.TypeFor[json.Marshaler]()
	unmarshalerType     = reflect.TypeFor[json.Unmarshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	jsonMapType         = reflect.TypeFor[safemap.JSONMap[any]]()
	safeName            = regexp.MustCompile(`[^a-zA-Z0-9._-]`)
	packagePath         = regexp.MustCompile(`[\w.~-]*/`)
	packageName         = regexp.MustCompile(`[\p{L}\p{N}_]+\.`)
	nameToken           = regexp.MustCompile(`[\p{L}\p{N}_]+`)
)

// schemaKey identifies a component: request schemas are those of decoded bodies, which keep only json-sourced
// fields and add validation constraints, and response schemas those of encoded results.
type schemaKey struct {
	t       reflect.Type
	request bool
}

type schemaBuilder struct {
	overrides map[reflect.Type]Schema
	// components holds the component schemas under temporary names until finish names them.
	components map[string]Schema
	names      map[schemaKey]string
	building   map[schemaKey]bool
}

func newSchemaBuilder(overrides map[reflect.Type]Schema) *schemaBuilder {
	return &schemaBuilder{overrides: overrides, components: make(map[string]Schema), names: make(map[schemaKey]string),
		building: make(map[schemaKey]bool)}
}

// override returns a copy of the WithSchema schema of t.
func (b *schemaBuilder) override(t reflect.Type) (Schema, bool) {
	s, ok := b.overrides[t]
	return maps.Clone(s), ok
}

// codecs returns the interfaces encoding/json uses for t in one direction: request bodies are decoded and
// responses are encoded.
func codecs(request bool) (jsonCodec, textCodec reflect.Type) {
	if request {
		return unmarshalerType, textUnmarshalerType
	}
	return marshalerType, textMarshalerType
}

// implements reports whether t or *t implements iface, because encoding/json uses the methods of both.
func implements(t, iface reflect.Type) bool {
	return t.Implements(iface) || reflect.PointerTo(t).Implements(iface)
}

// leaf reports whether the struct t is documented as one value rather than by its fields.
func (b *schemaBuilder) leaf(t reflect.Type, request bool) bool {
	if _, ok := b.overrides[t]; ok {
		return true
	}
	jsonCodec, textCodec := codecs(request)
	return t == timeType || t == numberType || t == rawType || implements(t, jsonCodec) || implements(t, textCodec)
}

// jsonMapValue returns V when t is a safemap.JSONMap[V]. A JSONMap has its own json.Marshaler, but encodes as the
// map[string]V its Clone returns.
func jsonMapValue(t reflect.Type) (reflect.Type, bool) {
	if t.Kind() != reflect.Struct || t.PkgPath() != jsonMapType.PkgPath() || !strings.HasPrefix(t.Name(), "JSONMap[") {
		return nil, false
	}
	clone, _ := reflect.PointerTo(t).MethodByName("Clone")
	return clone.Type.Out(0).Elem(), true
}

func nullable(s Schema) Schema { return Schema{"anyOf": []Schema{s, {"type": "null"}}} }

func ref(name string) Schema { return Schema{"$ref": componentRef + name} }

// primitive returns the schema of a boolean, string or number kind.
func primitive(t reflect.Type) (Schema, bool) {
	switch t.Kind() {
	case reflect.Bool:
		return Schema{"type": "boolean"}, true
	case reflect.String:
		return Schema{"type": "string"}, true
	case reflect.Int, reflect.Int8, reflect.Int16:
		return Schema{"type": "integer"}, true
	case reflect.Int32:
		return Schema{"type": "integer", "format": "int32"}, true
	case reflect.Int64:
		return Schema{"type": "integer", "format": "int64"}, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		return Schema{"type": "integer", "minimum": uint64(0)}, true
	case reflect.Uint64:
		return Schema{"type": "integer", "minimum": uint64(0), "format": "uint64"}, true
	case reflect.Float32:
		return Schema{"type": "number", "format": "float"}, true
	case reflect.Float64:
		return Schema{"type": "number", "format": "double"}, true
	}
	return nil, false
}

// schema documents the JSON encoding of t in a request body (request) or a response.
func (b *schemaBuilder) schema(t reflect.Type, request bool) (Schema, error) {
	if s, ok := b.override(t); ok {
		return s, nil
	}
	if t.Kind() == reflect.Pointer {
		s, err := b.schema(t.Elem(), request)
		if err != nil {
			return nil, err
		}
		return nullable(s), nil
	}
	switch t {
	case timeType:
		return Schema{"type": "string", "format": "date-time"}, nil
	case numberType:
		return Schema{"type": "number"}, nil
	case rawType:
		return Schema{}, nil
	}
	// A JSONMap is held by pointer, which makes it nullable, so the object itself is not.
	if v, ok := jsonMapValue(t); ok {
		value, err := b.schema(v, request)
		if err != nil {
			return nil, err
		}
		return Schema{"type": "object", "additionalProperties": value}, nil
	}
	jsonCodec, textCodec := codecs(request)
	if implements(t, jsonCodec) {
		return nil, fmt.Errorf("type %s implements %s, so its JSON shape cannot be inferred: describe it with WithSchema", t, jsonCodec)
	}
	if implements(t, textCodec) {
		return Schema{"type": "string"}, nil
	}
	if s, ok := primitive(t); ok {
		return s, nil
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return Schema{"type": "string", "contentEncoding": "base64"}, nil
		}
		items, err := b.schema(t.Elem(), request)
		if err != nil {
			return nil, err
		}
		if t.Kind() == reflect.Slice {
			return nullable(Schema{"type": "array", "items": items}), nil
		}
		return Schema{"type": "array", "items": items, "minItems": t.Len(), "maxItems": t.Len()}, nil
	case reflect.Map:
		key := t.Key()
		if key.Kind() != reflect.String && !isInteger(key.Kind()) && !implements(key, textCodec) {
			return nil, fmt.Errorf("map key type %s is not JSON-compatible", key)
		}
		value, err := b.schema(t.Elem(), request)
		if err != nil {
			return nil, err
		}
		return nullable(Schema{"type": "object", "additionalProperties": value}), nil
	case reflect.Interface:
		return Schema{}, nil
	case reflect.Struct:
		key := schemaKey{t, request}
		if name, ok := b.names[key]; ok {
			return ref(name), nil
		}
		if t.Name() == "" {
			if b.building[key] {
				return nil, fmt.Errorf("recursive unnamed type %s", t)
			}
			b.building[key] = true
			defer delete(b.building, key)
			return b.object(t, request)
		}
		name := "@" + strconv.Itoa(len(b.names)) // a temporary name, until finish names the components
		b.names[key] = name                      // registered before descending, for recursive types
		obj, err := b.object(t, request)
		if err != nil {
			return nil, err
		}
		b.components[name] = obj
		return ref(name), nil
	}
	return nil, fmt.Errorf("unsupported Go type %s", t)
}

// paramSchema documents a path, header or query value of type t, and reports whether it is repeated: a slice or
// an array takes one value per repetition. A parameter is never null, so a pointer only makes it optional.
func (b *schemaBuilder) paramSchema(t reflect.Type) (Schema, bool, error) {
	if s, ok := b.paramValue(t); ok {
		return s, false, nil
	}
	t = indirect(t)
	if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		return nil, false, fmt.Errorf("unsupported textual type %s", t)
	}
	items, ok := b.paramValue(t.Elem())
	if !ok {
		return nil, false, fmt.Errorf("unsupported textual type %s: a repeated parameter holds text values only", t)
	}
	s := Schema{"type": "array", "items": items}
	if t.Kind() == reflect.Array {
		s["minItems"], s["maxItems"] = t.Len(), t.Len()
	}
	return s, true, nil
}

// paramValue documents one text value of type t: a type with a WithSchema, time.Time, an
// encoding.TextUnmarshaler, which the binder decodes text with, or a primitive.
func (b *schemaBuilder) paramValue(t reflect.Type) (Schema, bool) {
	for {
		if s, ok := b.override(t); ok {
			return s, true
		}
		if t.Kind() != reflect.Pointer {
			break
		}
		t = t.Elem()
	}
	if t == timeType {
		return Schema{"type": "string", "format": "date-time"}, true
	}
	if implements(t, textUnmarshalerType) {
		return Schema{"type": "string"}, true
	}
	return primitive(t)
}

func isInteger(k reflect.Kind) bool { return k >= reflect.Int && k <= reflect.Uintptr }

type jsonField struct {
	field reflect.StructField
	name  string
	// note describes the other sources of a request field bound from several, which are all shared.
	note     string
	depth    int
	tagged   bool
	optional bool
	quoted   bool
	shared   bool
}

// fields implements JSON embedding dominance: shallow fields win, a tagged
// field wins over untagged peers, and equally dominant conflicts disappear.
// In request mode, only fields with a json source participate.
func (b *schemaBuilder) fields(t reflect.Type, request bool) ([]jsonField, error) {
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
			if tag == "-" {
				continue // "-," names the field "-"
			}
			parts := strings.Split(tag, ",")
			name := parts[0]
			if name != "" && !validJSONName(name) {
				name = ""
			}
			var note string
			shared := false
			if request {
				srcs, err := sources(f)
				if err != nil {
					return err
				}
				j := slices.IndexFunc(srcs, func(s fieldSource) bool { return s.kind == "json" })
				if j < 0 {
					if len(srcs) == 0 && f.Anonymous && ft.Kind() == reflect.Struct {
						if err := walk(ft, depth+1, optional || f.Type.Kind() == reflect.Pointer); err != nil {
							return err
						}
					}
					continue
				}
				note, shared = sourceNote(srcs, j), len(srcs) > 1
			}
			if f.Anonymous && name == "" && ft.Kind() == reflect.Struct && !b.leaf(ft, request) {
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
			jf := jsonField{field: f, name: name, note: note, depth: depth, tagged: tagged, optional: optional, shared: shared}
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
	fs, err := b.fields(t, request)
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
			s, need, err = b.validation(s, f.field.Type, f.field.Tag.Get("validate"), true)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", f.field.Name, err)
			}
			if f.shared {
				need = false // any of the field's sources satisfies the validation
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
		if f.note != "" {
			s = maps.Clone(s)
			description, _ := s["description"].(string)
			s["description"] = joinNotes(description, f.note)
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

// finish names the components and stores them in doc. A component is named after its Go type: Product, or
// PageProduct for Page[shop.Product]. The request schema of a type that also has a response schema gets the
// suffix Request. Names that still collide get the package name as a prefix (ShopProduct), and names that
// collide even then get a hash of the type as a suffix instead (Product_1a2b3c4d5e6f). A name therefore depends
// only on the types in the document, not on the order of the routes.
func (b *schemaBuilder) finish(doc *Document) {
	responses := make(map[reflect.Type]bool)
	for key := range b.names {
		if !key.request {
			responses[key.t] = true
		}
	}
	base := make(map[schemaKey]string, len(b.names))
	for key := range b.names {
		name := typeName(key.t)
		if key.request && responses[key.t] {
			name += "Request"
		}
		base[key] = name
	}
	final := maps.Clone(base)
	for _, rename := range []func(schemaKey) string{
		func(key schemaKey) string {
			return safeName.ReplaceAllString(capitalize(path.Base(key.t.PkgPath())), "_") + capitalize(base[key])
		},
		func(key schemaKey) string { return base[key] + "_" + typeHash(key) },
	} {
		groups := make(map[string][]schemaKey)
		for key, name := range final {
			groups[name] = append(groups[name], key)
		}
		for _, group := range groups {
			if len(group) > 1 {
				for _, key := range group {
					final[key] = rename(key)
				}
			}
		}
	}
	renames := make(map[string]string, len(final))
	doc.Components.Schemas = make(map[string]Schema, len(final))
	for key, name := range final {
		renames[b.names[key]] = name
		doc.Components.Schemas[name] = b.components[b.names[key]]
	}
	for _, methods := range doc.Paths {
		for _, op := range methods {
			for _, p := range op.Parameters {
				renameRefs(p.Schema, renames)
			}
			if op.RequestBody != nil {
				for _, media := range op.RequestBody.Content {
					renameRefs(media.Schema, renames)
				}
			}
			for _, response := range op.Responses {
				for _, header := range response.Headers {
					renameRefs(header.Schema, renames)
				}
				for _, media := range response.Content {
					renameRefs(media.Schema, renames)
				}
			}
		}
	}
	for _, s := range doc.Components.Schemas {
		renameRefs(s, renames)
	}
}

// renameRefs replaces the temporary component names of the references in v with their final names.
func renameRefs(v any, renames map[string]string) {
	switch v := v.(type) {
	case Schema:
		if r, ok := v["$ref"].(string); ok {
			if name, ok := renames[strings.TrimPrefix(r, componentRef)]; ok {
				v["$ref"] = componentRef + name
			}
		}
		for _, value := range v {
			renameRefs(value, renames)
		}
	case []Schema:
		for _, s := range v {
			renameRefs(s, renames)
		}
	case map[string]Schema:
		for _, s := range v {
			renameRefs(s, renames)
		}
	}
}

// typeName returns a component name for the named type t: its name, followed by the names of its type arguments
// without their packages, so Page[shop.Product] is PageProduct.
func typeName(t reflect.Type) string {
	name, args, generic := strings.Cut(t.Name(), "[")
	if generic {
		args = packageName.ReplaceAllString(packagePath.ReplaceAllString(args, ""), "")
		for _, token := range nameToken.FindAllString(args, -1) {
			name += capitalize(token)
		}
	}
	return safeName.ReplaceAllString(name, "_")
}

// typeHash identifies the component of key among types with the same name.
func typeHash(key schemaKey) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%t", key.t.PkgPath(), key.t, key.request)))
	return fmt.Sprintf("%x", hash[:6])
}

func capitalize(s string) string {
	if s == "" {
		return ""
	}
	r, size := utf8.DecodeRuneInString(s)
	return string(unicode.ToUpper(r)) + s[size:]
}
