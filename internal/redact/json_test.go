package redact

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

var testNames = []string{"password", "token", "card", "pin", "k\U0001F600", "\b\f\n\r\t/\\\""}

func maskJSON(fields Fields, src string) (string, bool) {
	m := jsonMasker{fields: fields, src: []byte(src)}
	masked, valid := m.mask()
	return string(masked), valid
}

func TestMaskJSON(t *testing.T) {
	fields := NewFields(testNames...)
	for _, tc := range []struct {
		name, src, want string
	}{
		{"nothing sensitive", `{"user":"ada","age":36}`, `{"user":"ada","age":36}`},
		{"member", `{"user":"ada","password":"hunter2"}`, `{"user":"ada","password":"[*]"}`},
		{"whitespace dropped", "{ \"password\" :\t\"x\" ,\n\"a\" : 1 }", `{"password":"[*]","a":1}`},
		{"pretty", "\n{\n  \"user\": \"ada\",\r\n  \"tags\": [ 1, 2 ]\n}\n", `{"user":"ada","tags":[1,2]}`},
		{"whitespace in strings kept", `{"a":" x\ty "}`, `{"a":" x\ty "}`},
		{"whitespace in masked value", "{\"card\": { \"a\" : [ 1 ] } , \"b\": 2}", `{"card":"[*]","b":2}`},
		{"nested", `{"user":{"name":"ada","password":"x"}}`, `{"user":{"name":"ada","password":"[*]"}}`},
		{"in arrays", `[{"token":"a"},{"token":"b"},1]`, `[{"token":"[*]"},{"token":"[*]"},1]`},
		{"number", `{"pin":1234}`, `{"pin":"[*]"}`},
		{"object", `{"card":{"number":"4111","token":"x"},"a":1}`, `{"card":"[*]","a":1}`},
		{"array", `{"card":[4,1,1,1]}`, `{"card":"[*]"}`},
		{"literals", `{"pin":null,"token":true,"a":[false,null,true]}`, `{"pin":"[*]","token":"[*]","a":[false,null,true]}`},
		{"case folded", `{"PASSWORD":"x","paſſword":"y"}`, `{"PASSWORD":"[*]","paſſword":"[*]"}`},
		{"escaped and case folded", `{"pa\u017f\u017fword":"x"}`, `{"pa\u017f\u017fword":"[*]"}`},
		{"escaped key", `{"pass\u0077ord":"x"}`, `{"pass\u0077ord":"[*]"}`},
		{"upper-case hex in key", `{"PASSW\u004FRD":"x"}`, `{"PASSW\u004FRD":"[*]"}`},
		{"escapes in key", `{"\b\f\n\r\t\/\\\"":"x","pin\u0000":"y"}`, `{"\b\f\n\r\t\/\\\"":"[*]","pin\u0000":"y"}`},
		{"surrogate pair key", `{"k\ud83d\ude00":"x"}`, `{"k\ud83d\ude00":"[*]"}`},
		{"escaped quote in key", `{"a\"b":"x","token":"y"}`, `{"a\"b":"x","token":"[*]"}`},
		{"not a key", `{"a":"\"token\":\"x\"","b":["password"]}`, `{"a":"\"token\":\"x\"","b":["password"]}`},
		{"sensitive name as value", `"password"`, `"password"`},
		{"primitive", `-12.5e+3`, `-12.5e+3`},
		{"empty containers", `{"a":{},"b":[],"token":{}}`, `{"a":{},"b":[],"token":"[*]"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, valid := maskJSON(fields, tc.src)
			if got != tc.want || !valid {
				t.Fatalf("got %s (valid %v), want %s", got, valid, tc.want)
			}
		})
	}
}

// TestMaskJSONInvalid covers bodies cut at the logging limit or that are not JSON at all: they are
// masked up to the first byte that is not valid JSON, and never show part of a sensitive value.
func TestMaskJSONInvalid(t *testing.T) {
	fields := NewFields(testNames...)
	for _, tc := range []struct {
		name, src, want string
	}{
		{"cut in sensitive string", `{"password":"hun`, `{"password":"[*]"`},
		{"cut in sensitive number", `{"pin":12`, `{"pin":"[*]"`},
		{"cut in sensitive object", `{"card":{"number":"41`, `{"card":"[*]"`},
		{"cut in sensitive escape", `{"token":"ab\u00`, `{"token":"[*]"`},
		{"cut after colon", `{"password":`, `{"password":"[*]"`},
		{"cut after key", `{"password"`, `{"password"`},
		{"cut in key", `{"passw`, `{"passw`},
		{"cut elsewhere", `{"user":"ad`, `{"user":"ad`},
		{"cut literal", `{"a":tr`, `{"a":`},
		{"not json", `password=hunter2&a=1`, ``},
		{"syntax error", `{"a":1 "password":"x"}`, `{"a":1`},
		{"trailing data", `{"a":1} {"password":"x"}`, `{"a":1}`},
		{"cut in whitespace", "{\"a\": [1, \n", `{"a":[1,`},
		{"invalid escape", `{"a":"\q","password":"x"}`, `{"a":"`},
		{"control character", "{\"a\":\"\x01\",\"password\":\"x\"}", `{"a":"`},
		{"leading zero", `{"a":01}`, `{"a":0`},
		{"bad fraction", `[1.]`, `[1.`},
		{"bad exponent", `[1e+]`, `[1e+`},
		{"lone minus", `[-]`, `[-`},
		{"too deep array", strings.Repeat("[", maxDepth+1) + strings.Repeat("]", maxDepth+1), strings.Repeat("[", maxDepth)},
		{"too deep object", strings.Repeat(`{"a":`, maxDepth+1) + "1" + strings.Repeat("}", maxDepth+1), strings.Repeat(`{"a":`, maxDepth)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, valid := maskJSON(fields, tc.src)
			if got != tc.want || valid {
				t.Fatalf("got %s (valid %v), want %s", got, valid, tc.want)
			}
		})
	}

	deepest := strings.Repeat("[", maxDepth) + strings.Repeat("]", maxDepth)
	if _, valid := maskJSON(fields, deepest); !valid {
		t.Fatalf("JSON nested %d deep is invalid", maxDepth)
	}
}

// TestMaskJSONReturnsSource checks that nothing is copied when nothing is masked.
func TestMaskJSONReturnsSource(t *testing.T) {
	src := []byte(`{"user":"ada"}`)
	m := jsonMasker{fields: NewFields("password"), src: src}
	if masked, _ := m.mask(); &masked[0] != &src[0] {
		t.Fatal("unmasked JSON was copied")
	}
}

// FuzzMaskJSON checks the masker against encoding/json: it accepts exactly the JSON json.Valid
// does, masks exactly the members a reference walk over the decoded value masks, compacts it the
// way json.Compact does, and masks a cut document into a prefix of the masked whole, so a cut body
// never shows part of a sensitive value. Whatever the input, the output holds no line break.
func FuzzMaskJSON(f *testing.F) {
	for _, seed := range []string{
		`{"user":"ada","password":"hunter2"}`,
		`[{"token":"a","nested":{"PIN":1,"x":[true,null]}},-1.5e3]`,
		`{"pass\u0077ord":"x","k\ud83d\ude00":{"a":"\"}"}}`,
		`{"a":01}`,
		"\"\xff\"",
		"{\n  \"password\" : [ 1 ],\r\n\t\"a\": \" b \"\n}\n",
	} {
		f.Add([]byte(seed))
	}
	fields := NewFields(testNames...)

	f.Fuzz(func(t *testing.T, src []byte) {
		m := jsonMasker{fields: fields, src: src}
		masked, valid := m.mask()
		if bytes.ContainsAny(masked, "\n\r") {
			t.Fatalf("masked %q holds a line break", masked)
		}
		if !json.Valid(src) {
			if valid {
				t.Fatalf("invalid JSON %q is valid", src)
			}
			return
		}
		if !valid {
			if bytes.Count(src, []byte("["))+bytes.Count(src, []byte("{")) <= maxDepth {
				t.Fatalf("valid JSON %q is invalid", src)
			}
			return
		}

		if got, want := decode(t, masked), maskReference(decode(t, src)); !reflect.DeepEqual(got, want) {
			t.Fatalf("masked %s\ngot  %#v\nwant %#v", masked, got, want)
		}

		var compact bytes.Buffer
		if err := json.Compact(&compact, masked); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(compact.Bytes(), masked) {
			t.Fatalf("masked %s is not compact", masked)
		}

		whole := string(masked)
		for i := range len(src) {
			if part, _ := maskJSON(fields, string(src[:i])); !strings.HasPrefix(whole, part) {
				t.Fatalf("masked prefix %s of %q is no prefix of %s", part, src[:i], whole)
			}
		}
	})
}

func decode(t *testing.T, data []byte) any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		t.Fatalf("decode %q: %v", data, err)
	}
	return v
}

// maskReference masks the decoded JSON v, matching keys with strings.EqualFold.
func maskReference(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for key, value := range v {
			v[key] = maskReference(value)
			for _, name := range testNames {
				if strings.EqualFold(key, name) {
					v[key] = Mask
				}
			}
		}
	case []any:
		for i, value := range v {
			v[i] = maskReference(value)
		}
	}
	return v
}

func TestLogJSON(t *testing.T) {
	fields := NewFields("password")
	for _, tc := range []struct {
		name      string
		fields    Fields
		body      string
		truncated bool
		want      string
	}{
		{"empty", fields, ``, false, `{}`},
		{"raw", Fields{}, `{"password":"x"}`, false, `{"body":{"password":"x"}}`},
		{"raw pretty", Fields{}, "{\n  \"a\": [1, 2]\n}\n", false, `{"body":{"a":[1,2]}}`},
		{"raw truncated", Fields{}, `{"password":"x`, true, `{"body":"{\"password\":\"x","body_cut":true}`},
		{"raw not json", Fields{}, `password=x`, false, `{"body":"password=x"}`},
		{"raw syntax error", Fields{}, `{"a":1 "b":2}`, false, `{"body":"{\"a\":1 \"b\":2}"}`},
		{"raw binary", Fields{}, "\x00\xff", false, `{"body":"\u0000\ufffd"}`},
		{"masked", fields, `{"password":"x"}`, false, `{"body":{"password":"[*]"}}`},
		{"masked pretty", fields, "{\n  \"password\": \"x\",\n  \"a\": 1\n}", false, `{"body":{"password":"[*]","a":1}}`},
		{"masked pretty truncated", fields, "{\n  \"a\": 1,\n  \"pass", true, `{"body":"{\"a\":1,\"pass","body_cut":true}`},
		{"masked truncated", fields, `{"password":"x`, true, `{"body":"{\"password\":\"[*]\"","body_cut":true}`},
		{"masked valid but truncated", fields, `12`, true, `{"body":"12","body_cut":true}`},
		{"masked not json", fields, `password=x`, false, `{"body":"","body_cut":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			lg := zerolog.New(&out)
			e := lg.Log()
			tc.fields.LogJSON(e, "body", []byte(tc.body), tc.truncated)
			e.Send()
			if got := strings.TrimSpace(out.String()); got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
			if !json.Valid(out.Bytes()) || bytes.Count(out.Bytes(), []byte("\n")) != 1 {
				t.Fatalf("log line %s is not one line of JSON", out.Bytes())
			}
		})
	}
}

func TestLogJSONDisabledEvent(t *testing.T) {
	var out bytes.Buffer
	lg := zerolog.New(&out).Level(zerolog.WarnLevel)
	for _, fields := range []Fields{{}, NewFields("password")} {
		fields.LogJSON(nil, "body", []byte(`{"password":"x"}`), false)
		e := lg.Info()
		fields.LogJSON(e, "body", []byte(`{"password":"x"}`), false)
		e.Send()
	}
	if out.Len() != 0 {
		t.Fatalf("disabled event logged %s", out.Bytes())
	}
}

func TestLogJSONDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items under the race detector")
	}
	lg := zerolog.New(io.Discard)
	for _, fields := range []Fields{{}, NewFields("password", "token")} {
		for _, body := range [][]byte{
			[]byte(benchBody),
			[]byte(prettyBody),
			[]byte(`{"password":"hunter2","nested":{"token":"x"}}`),
			[]byte(`password=hunter2&token=x`),
		} {
			for _, truncated := range []bool{false, true} {
				allocs := testing.AllocsPerRun(100, func() {
					e := lg.Info()
					fields.LogJSON(e, "body", body, truncated)
					e.Send()
				})
				if allocs != 0 {
					t.Errorf("LogJSON(%.20q..., %v) with %v fields allocated %v times", body, truncated, len(fields.names), allocs)
				}
			}
		}
	}
}

// benchBody is a typical request body of about 1 KiB.
const benchBody = `{"order_id":"ord_01J9Z3Q4","customer":{"id":"cus_8812","name":"Ada Lovelace","email":"ada@example.com",` +
	`"phone":"+44 20 7946 0958","address":{"line1":"12 Analytical St","city":"London","postcode":"N1 9GU","country":"GB"}},` +
	`"items":[{"sku":"SKU-1001","name":"Difference Engine","quantity":1,"unit_price":125000,"tags":["brass","gears"]},` +
	`{"sku":"SKU-1002","name":"Punch Cards","quantity":500,"unit_price":12,"tags":["paper"]},` +
	`{"sku":"SKU-1003","name":"Notes on the Engine","quantity":2,"unit_price":4999,"tags":["book","annotated"]}],` +
	`"payment":{"method":"card","card":{"brand":"visa","last4":"4242","exp_month":12,"exp_year":2031},"currency":"GBP"},` +
	`"shipping":{"method":"express","eta_days":2,"instructions":"Leave with the porter if nobody answers the door."},` +
	`"metadata":{"source":"web","campaign":"autumn-2026","referrer":"https://example.com/blog/engines","ab_bucket":7},` +
	`"notes":"Please gift wrap the book and include a handwritten card wishing a happy birthday.","total":136497}`

// prettyBody is a request body indented the way clients such as Postman send it.
const prettyBody = `{
  "order_id": "ord_01J9Z3Q4",
  "customer": {
    "id": "cus_8812",
    "email": "ada@example.com",
    "address": {"line1": "12 Analytical St", "city": "London", "country": "GB"}
  },
  "items": [
    {"sku": "SKU-1001", "quantity": 1, "unit_price": 125000, "tags": ["brass", "gears"]},
    {"sku": "SKU-1002", "quantity": 500, "unit_price": 12, "tags": ["paper"]}
  ],
  "payment": {"method": "card", "card": {"brand": "visa", "last4": "4242"}},
  "total": 136497
}
`

func BenchmarkLogJSON(b *testing.B) {
	lg := zerolog.New(io.Discard)
	for _, bc := range []struct {
		name   string
		fields Fields
		body   string
	}{
		{"no_fields", Fields{}, benchBody},
		{"no_match", NewFields("password", "token"), benchBody},
		{"masked", NewFields("password", "token", "card", "email"), benchBody},
		{"pretty_no_fields", Fields{}, prettyBody},
		{"pretty_masked", NewFields("password", "token", "card", "email"), prettyBody},
	} {
		b.Run(bc.name, func(b *testing.B) {
			body := []byte(bc.body)
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				e := lg.Info()
				bc.fields.LogJSON(e, "body", body, false)
				e.Send()
			}
		})
	}
}

func BenchmarkFieldsHas(b *testing.B) {
	fields := NewFields("password", "token", "authorization", "x-api-key")
	key := []byte("Content-Type")
	b.ReportAllocs()
	for b.Loop() {
		fields.Has("Authorization")
		fields.Has(key)
	}
}
