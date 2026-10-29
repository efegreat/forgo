package safemap

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
)

var (
	_ json.Marshaler   = (*JSONMap[any])(nil)
	_ json.Unmarshaler = (*JSONMap[any])(nil)
	_ driver.Valuer    = (*JSONMap[any])(nil)
	_ sql.Scanner      = (*JSONMap[any])(nil)
)

func TestJSONMapMarshalJSON(t *testing.T) {
	type envelope struct {
		Headers *JSONMap[string] `json:"headers"`
	}
	tests := []struct {
		name string
		in   envelope
		want string
	}{
		{"entries with sorted keys", envelope{NewJSONMapWithData(map[string]string{"b": "2", "a": "1"})}, `{"headers":{"a":"1","b":"2"}}`},
		{"empty", envelope{NewJSONMap[string]()}, `{"headers":{}}`},
		{"no map", envelope{new(JSONMap[string])}, `{"headers":null}`},
		{"nil", envelope{}, `{"headers":null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.in)
			if err != nil || string(got) != tt.want {
				t.Fatalf("json.Marshal = %s, %v; want %s", got, err, tt.want)
			}
		})
	}

	if got, err := (*JSONMap[string])(nil).MarshalJSON(); err != nil || string(got) != "null" {
		t.Fatalf("nil MarshalJSON() = %s, %v; want null", got, err)
	}
}

func TestJSONMapUnmarshalJSON(t *testing.T) {
	var dst struct {
		Meta *JSONMap[any] `json:"meta"`
	}
	in := `{"meta":{"id":` + strconv.FormatUint(math.MaxUint64, 10) + `,"tags":["a"],"nested":{"n":1.5}}}`
	if err := json.Unmarshal([]byte(in), &dst); err != nil {
		t.Fatal(err)
	}
	if got := dst.Meta.GetTyped[uint64]("id"); got != math.MaxUint64 {
		t.Fatalf("GetTyped[uint64](id) = %d, want %d", got, uint64(math.MaxUint64))
	}
	nested := dst.Meta.GetTyped[map[string]any]("nested")
	if n, ok := nested["n"].(json.Number); !ok || n != "1.5" {
		t.Fatalf("nested number = %#v, want json.Number(1.5)", nested["n"])
	}
	out, err := json.Marshal(dst)
	if err != nil || string(out) != `{"meta":{"id":18446744073709551615,"nested":{"n":1.5},"tags":["a"]}}` {
		t.Fatalf("round trip = %s, %v", out, err)
	}

	m := NewJSONMapWithData(map[string]any{"old": 1})
	if err := m.UnmarshalJSON([]byte(`{"new":2}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("old"); ok || m.Len() != 1 {
		t.Fatalf("UnmarshalJSON kept old entries: %v", m.Clone())
	}

	if err := m.UnmarshalJSON([]byte(`["not an object"]`)); err == nil {
		t.Fatal("UnmarshalJSON accepted an array")
	}
	if _, ok := m.Get("new"); !ok {
		t.Fatal("a failed UnmarshalJSON changed the map")
	}

	if err := m.UnmarshalJSON([]byte(`null`)); err != nil {
		t.Fatal(err)
	}
	if v, err := m.Value(); err != nil || v != nil {
		t.Fatalf("Value() after null = %v, %v; want NULL", v, err)
	}

	if err := (*JSONMap[any])(nil).UnmarshalJSON([]byte(`{}`)); err == nil {
		t.Fatal("UnmarshalJSON on a nil receiver succeeded")
	}
}

func TestJSONMapValue(t *testing.T) {
	tests := []struct {
		name    string
		m       *JSONMap[any]
		want    driver.Value
		wantErr string
	}{
		{"nil", nil, nil, ""},
		{"no map", new(JSONMap[any]), nil, ""},
		{"empty", NewJSONMap[any](), "{}", ""},
		{"entries", NewJSONMapWithData(map[string]any{"b": json.Number("2"), "a": "x"}), `{"a":"x","b":2}`, ""},
		{"unsupported value", NewJSONMapWithData(map[string]any{"f": func() {}}), nil, "json map: marshal: "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.m.Value()
			if tt.wantErr != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tt.wantErr) {
					t.Fatalf("Value() error = %v, want prefix %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("Value() = %#v, %v; want %#v", got, err, tt.want)
			}
			if !driver.IsValue(got) {
				t.Fatalf("Value() = %T, not a driver.Value", got)
			}
		})
	}
}

func TestJSONMapScan(t *testing.T) {
	tests := []struct {
		name    string
		src     any
		want    map[string]any
		wantErr string
	}{
		{"bytes", []byte(`{"a":"x"}`), map[string]any{"a": "x"}, ""},
		{"string", `{"a":"x"}`, map[string]any{"a": "x"}, ""},
		{"surrounding white space", " \n{\"a\":\"x\"}\n ", map[string]any{"a": "x"}, ""},
		{"numbers stay json.Number", `{"n":18446744073709551615}`, map[string]any{"n": json.Number("18446744073709551615")}, ""},
		{"NULL", nil, nil, ""},
		{"JSON null", `null`, nil, ""},
		{"trailing data", `{"a":"x"} junk`, nil, "json map: decode: data after the JSON object"},
		{"second object", `{"a":"x"}{"b":"y"}`, nil, "json map: decode: data after the JSON object"},
		{"array", `["a"]`, nil, "json map: decode: "},
		{"invalid", `{"a":`, nil, "json map: decode: "},
		{"empty", ``, nil, "json map: decode: EOF"},
		{"unsupported source", 42, nil, "json map: unsupported Scan source type int"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewJSONMapWithData(map[string]any{"old": "value"})
			err := m.Scan(tt.src)
			if tt.wantErr != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tt.wantErr) {
					t.Fatalf("Scan(%v) error = %v, want prefix %q", tt.src, err, tt.wantErr)
				}
				if v, _ := m.Get("old"); v != "value" || m.Len() != 1 {
					t.Fatalf("a failed Scan changed the map: %v", m.Clone())
				}
				return
			}
			if err != nil {
				t.Fatalf("Scan(%v) = %v", tt.src, err)
			}
			got := m.Clone()
			if len(got) != len(tt.want) || (got == nil) != (tt.want == nil) {
				t.Fatalf("Scan(%v) left %#v, want %#v", tt.src, got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Fatalf("Scan(%v) left %#v, want %#v", tt.src, got, tt.want)
				}
			}
			if _, ok := m.Get("old"); ok {
				t.Fatal("Get(old) found a key after Scan replaced the map")
			}
		})
	}

	if err := (*JSONMap[any])(nil).Scan(`{}`); err == nil {
		t.Fatal("Scan on a nil receiver succeeded")
	}
}

// TestJSONMapRoundTrip stores a JSONMap and reads it back, as an outbox row does.
func TestJSONMapRoundTrip(t *testing.T) {
	for _, in := range []*JSONMap[string]{
		NewJSONMapWithData(map[string]string{"x-msg-id": "1", "tenant": "acme"}),
		NewJSONMap[string](),
		new(JSONMap[string]),
	} {
		stored, err := in.Value()
		if err != nil {
			t.Fatal(err)
		}
		out := NewJSONMap[string]()
		if err := out.Scan(stored); err != nil {
			t.Fatal(err)
		}
		if again, _ := out.Value(); again != stored {
			t.Fatalf("round trip of %#v stored %#v", stored, again)
		}
	}
}

// TestJSONMapScanUpdatesLookups guards the lock-free empty check: Scan replaces data and must update its size.
func TestJSONMapScanUpdatesLookups(t *testing.T) {
	m := NewJSONMap[any]()
	if err := m.Scan([]byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("a"); !ok {
		t.Fatal("Get(a) missed a scanned key")
	}
	if err := m.Scan(nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("Get(a) found a key after scanning NULL")
	}
}

func BenchmarkJSONMap(b *testing.B) {
	headers := map[string]string{"x-msg-id": "0192f5a4-8e1c-7c2e-9b1a-3c4d5e6f7a8b", "x-msg-name": "order.placed", "tenant": "acme"}
	b.Run("value", func(b *testing.B) {
		m := NewJSONMapWithData(headers)
		b.ReportAllocs()
		for b.Loop() {
			sinkValue, _ = m.Value()
		}
	})
	b.Run("scan", func(b *testing.B) {
		stored, _ := NewJSONMapWithData(headers).Value()
		src := []byte(stored.(string))
		b.ReportAllocs()
		for b.Loop() {
			m := new(JSONMap[string])
			_ = m.Scan(src)
		}
	})
}
