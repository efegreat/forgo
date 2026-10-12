package openapi

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidationRules(t *testing.T) {
	str, integer := reflect.TypeFor[string](), reflect.TypeFor[int]()
	for _, tc := range []struct {
		t           reflect.Type
		tag         string
		constraints string // the JSON of the constraints, empty for none
		required    bool
		err         string
	}{
		{t: str, tag: "required", required: true},
		{t: str, tag: "omitempty,required"},
		{t: str, tag: "required,omitempty", required: true},
		{t: str, tag: "-"},
		{t: str, tag: "min=2,max=5", constraints: `{"maxLength":5,"minLength":2}`},
		{t: str, tag: "len=3", constraints: `{"maxLength":3,"minLength":3}`},
		{t: str, tag: "gt=1,lt=4", constraints: `{"maxLength":3,"minLength":2}`},
		{t: str, tag: "gte=1,lte=4", constraints: `{"maxLength":4,"minLength":1}`},
		{t: str, tag: "lt=0", err: `impossible validation "lt=0"`},
		{t: str, tag: "min=-1", err: `invalid validation "min=-1"`},
		{t: str, tag: "min=x", err: `invalid validation "min=x"`},
		{t: str, tag: "oneof='a b' c", constraints: `{"enum":["a b","c"]}`},
		{t: str, tag: "oneof='a", err: "unterminated quoted oneof value"},
		{t: str, tag: "email", constraints: `{"format":"email"}`},
		{t: str, tag: "uuid4", constraints: `{"format":"uuid"}`},
		{t: str, tag: "url", constraints: `{"format":"uri"}`},
		{t: str, tag: "hostname_rfc1123", constraints: `{"format":"hostname"}`},
		{t: str, tag: "ipv6", constraints: `{"format":"ipv6"}`},
		{t: str, tag: "ip", constraints: `{"anyOf":[{"format":"ipv4"},{"format":"ipv6"}]}`},
		{t: str, tag: "email|uuid"}, // OR rules are ignored
		{t: str, tag: "alpha,startswith=a"},
		{t: integer, tag: "min=1,max=10", constraints: `{"maximum":10,"minimum":1}`},
		{t: integer, tag: "gt=0,lt=10", constraints: `{"exclusiveMaximum":10,"exclusiveMinimum":0}`},
		{t: integer, tag: "len=7", constraints: `{"const":7}`},
		{t: integer, tag: "oneof=1 2", constraints: `{"enum":[1,2]}`},
		{t: integer, tag: "min=1.5", err: `invalid validation "min=1.5"`},
		{t: integer, tag: "email"}, // a string format does not apply to numbers
		{t: reflect.TypeFor[uint8](), tag: "max=-1", err: `invalid validation "max=-1"`},
		{t: reflect.TypeFor[float64](), tag: "gte=0.5,oneof=0.5 1e3", constraints: `{"enum":[0.5,1e3],"minimum":0.5}`},
		{t: reflect.TypeFor[[]int](), tag: "min=1,dive,min=5", constraints: `{"minItems":1}`},
		{t: reflect.TypeFor[[]int](), tag: "required", constraints: `{"not":{"type":"null"}}`, required: true},
		{t: reflect.TypeFor[[]byte](), tag: "max=4"}, // base64 length is not the byte count
		{t: reflect.TypeFor[map[string]int](), tag: "max=3,keys,len=2,endkeys", constraints: `{"maxProperties":3}`},
		{t: reflect.TypeFor[*int](), tag: "required,min=1", constraints: `{"minimum":1,"not":{"type":"null"}}`, required: true},
		{t: reflect.TypeFor[struct{ A int }](), tag: "required,min=1", required: true},
	} {
		t.Run(tc.t.String()+" "+tc.tag, func(t *testing.T) {
			b := newSchemaBuilder(nil)
			base, err := b.schema(tc.t, true)
			if err != nil {
				t.Fatal(err)
			}
			s, required, err := b.validation(base, tc.t, tc.tag, true)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("got %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if required != tc.required {
				t.Fatalf("required %v", required)
			}
			if tc.constraints == "" {
				if !sameJSON(t, s, base) {
					t.Fatalf("constraints added: %s", mustJSON(t, s))
				}
				return
			}
			all, ok := s["allOf"].([]Schema)
			if !ok || len(all) != 2 {
				t.Fatalf("schema %s", mustJSON(t, s))
			}
			if got := string(mustJSON(t, all[1])); got != tc.constraints {
				t.Fatalf("constraints %s, want %s", got, tc.constraints)
			}
		})
	}
}

func TestValidationWithoutNull(t *testing.T) {
	b := newSchemaBuilder(nil)
	s, required, err := b.validation(Schema{"type": "integer"}, reflect.TypeFor[*int](), "required", false)
	if err != nil || !required || string(mustJSON(t, s)) != `{"type":"integer"}` {
		t.Fatalf("parameter validation: %s %v %v", mustJSON(t, s), required, err)
	}
}

func sameJSON(t *testing.T, a, b Schema) bool {
	t.Helper()
	return string(mustJSON(t, a)) == string(mustJSON(t, b))
}
