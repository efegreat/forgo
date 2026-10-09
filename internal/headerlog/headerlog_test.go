package headerlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"forgo/logger"

	"github.com/rs/zerolog"
)

var trickyStrings = []string{
	"", "plain", "<a>&b", "x y z", "bad\xffutf8\xc3", "\x00\x01\b\f\n\r\t\x1f\x7f", `q"b\s`, "é日本😀",
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

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
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
// limit bytes are logged as a string with headers_cut.
func TestLogCut(t *testing.T) {
	const key = "headers"
	h := map[string]string{"accept": "*/*", "authorization": "Bearer x", "x-long": "0123456789"}
	want := mustMarshal(t, h)

	for limit := 0; limit <= len(want)+1; limit++ {
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
		if err := json.Unmarshal(got, &s); err != nil || s != string(want[:limit]) || !cut {
			t.Fatalf("limit %d: got %s (cut %v), want string %q with cut", limit, got, cut, want[:limit])
		}
	}
}
