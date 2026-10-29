package headerlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/efegreat/forgo/logger"

	"github.com/rs/zerolog"
)

var trickyStrings = []string{
	"", "plain", "<a>&b", `<https://api.example.com/items?page=2>; rel="next"`, "x y z", "bad\xffutf8\xc3", "\x00\x01\b\f\n\r\t\x1f\x7f", `q"b\s`, "é日本😀",
}

func appendString(buf []byte, _ string, value string) []byte {
	return AppendString(buf, value)
}

// list turns a map into headers in map iteration order, keeping nil as nil.
func list(m map[string]string) []Header[string] {
	if m == nil {
		return nil
	}
	headers := []Header[string]{}
	for name, value := range m {
		headers = append(headers, Header[string]{Name: name, Value: value})
	}
	return headers
}

// mustMarshal encodes v like an encoding/json Encoder without HTML escaping, which Append and
// AppendString match.
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func TestAppendStringMatchesJSON(t *testing.T) {
	for _, s := range trickyStrings {
		if got, want := AppendString(nil, s), mustMarshal(t, s); !bytes.Equal(got, want) {
			t.Errorf("AppendString(%q) = %s, want %s", s, got, want)
		}
	}
}

func FuzzAppendString(f *testing.F) {
	for _, s := range trickyStrings {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if got, want := AppendString(nil, s), mustMarshal(t, s); !bytes.Equal(got, want) {
			t.Fatalf("AppendString(%q) = %s, want %s", s, got, want)
		}
	})
}

func TestAppendMatchesJSON(t *testing.T) {
	tricky := map[string]string{}
	for i, s := range trickyStrings {
		tricky[s] = trickyStrings[len(trickyStrings)-1-i]
	}
	many := map[string]string{}
	for i := range 40 {
		many[fmt.Sprintf("x-header-%02d", i)] = fmt.Sprintf("value-%d", i)
	}

	cases := map[string]map[string]string{
		"nil":          nil,
		"empty":        {},
		"tricky":       tricky,
		"over 32 keys": many,
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if got, want := Append(nil, list(h), math.MaxInt, appendString), mustMarshal(t, h); !bytes.Equal(got, want) {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// TestLogCut checks every limit around the encoded size: the full JSON fits, otherwise its first
// limit bytes are logged as a string with headers_cut. A negative limit counts as 0.
func TestLogCut(t *testing.T) {
	const key = "headers"
	h := map[string]string{"accept": "*/*", "authorization": "Bearer x", "x-long": "0123456789"}
	want := mustMarshal(t, h)

	for limit := -1; limit <= len(want)+1; limit++ {
		var out bytes.Buffer
		lg := zerolog.New(&out)
		e := lg.Info()
		Log(e, key, list(h), limit, appendString)
		e.Send()

		var line map[string]json.RawMessage
		if err := json.Unmarshal(out.Bytes(), &line); err != nil {
			t.Fatalf("limit %d: invalid log line %s: %v", limit, out.Bytes(), err)
		}
		got := line[key]
		_, cut := line[logger.HeadersCutLogKey]

		if len(want) <= limit {
			if cut || !bytes.Equal(got, want) {
				t.Fatalf("limit %d: got %s (cut %v), want %s", limit, got, cut, want)
			}
			continue
		}
		var s string
		if err := json.Unmarshal(got, &s); err != nil || s != string(want[:max(limit, 0)]) || !cut {
			t.Fatalf("limit %d: got %s (cut %v), want string %q with cut", limit, got, cut, want[:max(limit, 0)])
		}
	}
}

func TestLogDisabledEvent(t *testing.T) {
	var out bytes.Buffer
	lg := zerolog.New(&out).Level(zerolog.WarnLevel)
	h := []Header[string]{{Name: "b", Value: "1"}, {Name: "a", Value: "2"}}

	Log(nil, "headers", h, math.MaxInt, appendString)
	e := lg.Info()
	Log(e, "headers", h, math.MaxInt, appendString)
	e.Send()
	if out.Len() != 0 || h[0].Name != "b" {
		t.Fatalf("disabled event logged %s or sorted headers %v", out.Bytes(), h)
	}
}

// benchHeaders are typical request headers.
var benchHeaders = []Header[string]{
	{Name: "Accept", Value: "application/json"},
	{Name: "Accept-Encoding", Value: "gzip, deflate, br"},
	{Name: "Authorization", Value: "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.c2lnbmF0dXJl"},
	{Name: "Content-Length", Value: "1024"},
	{Name: "Content-Type", Value: "application/json; charset=utf-8"},
	{Name: "Traceparent", Value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	{Name: "User-Agent", Value: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"},
	{Name: "X-Request-Id", Value: "01J9Z3Q4M8XKQ2V7T5R1N6B0C3"},
}

func TestLogDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items under the race detector")
	}
	lg := zerolog.New(io.Discard)
	h := append([]Header[string](nil), benchHeaders...)
	allocs := testing.AllocsPerRun(100, func() {
		e := lg.Info()
		Log(e, "headers", h, math.MaxInt, appendString)
		e.Send()
	})
	if allocs != 0 {
		t.Fatalf("Log allocated %v times", allocs)
	}
}

func BenchmarkLog(b *testing.B) {
	lg := zerolog.New(io.Discard)
	h := make([]Header[string], len(benchHeaders))
	b.ReportAllocs()
	for b.Loop() {
		copy(h, benchHeaders) // Log sorts in place
		e := lg.Info()
		Log(e, "headers", h, math.MaxInt, appendString)
		e.Send()
	}
}
