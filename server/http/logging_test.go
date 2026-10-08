package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
)

// marshalHeaders is how headers were logged before headerlog: clone, mask, json.Marshal.
func marshalHeaders(headers http.Header) []byte {
	result := headers.Clone()
	for name := range result {
		if _, sensitive := SensitiveHeaders[http.CanonicalHeaderKey(name)]; sensitive {
			result[name] = []string{"[*]"}
		}
	}
	payload, _ := json.Marshal(result)
	return payload
}

// loggedHeaders returns the JSON logHeaders writes for h.
func loggedHeaders(t *testing.T, h http.Header) []byte {
	t.Helper()
	var out bytes.Buffer
	lg := zerolog.New(&out)
	e := lg.Info()
	logHeaders(e, "headers", h, math.MaxInt)
	e.Send()

	var line map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("invalid log line %s: %v", out.Bytes(), err)
	}
	return line["headers"]
}

func manyHeaders(n int) http.Header {
	h := http.Header{}
	for i := range n {
		h.Set(fmt.Sprintf("X-Header-%02d", i), fmt.Sprintf("value-%d", i))
	}
	return h
}

func TestLoggedHeadersMatchJSON(t *testing.T) {
	cases := map[string]http.Header{
		"nil":          nil,
		"empty":        {},
		"masked":       {"Authorization": {"Bearer x"}, "cookie": {"a=b"}, "X-Api-Key": nil, "Accept": {"*/*"}},
		"nil values":   {"A": nil, "B": {}, "C": {"1", "2"}},
		"escaping":     {"X-Html": {"<a>&b"}, "X-Ctl": {"\x00\x01\b\f\n\r\t\x1f\x7f"}, "X-Sep": {"x y z"}},
		"unicode":      {"X-Utf8": {"é日本😀"}, "X-Bad": {"bad\xffutf8\xc3"}, "é": {"key"}},
		"over 32 keys": manyHeaders(40),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if got, want := loggedHeaders(t, h), marshalHeaders(h); !bytes.Equal(got, want) {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// FuzzLoggedHeaders covers masking: any spelling of a sensitive name must be masked like before.
func FuzzLoggedHeaders(f *testing.F) {
	f.Add("X-Test", "value")
	f.Add("authorization", "secret")
	f.Add("SET-COOKIE", "\xff< >&\x00")
	f.Fuzz(func(t *testing.T, name, value string) {
		h := http.Header{name: {value, name}, "Accept": {"*/*"}}
		if got, want := loggedHeaders(t, h), marshalHeaders(h); !bytes.Equal(got, want) {
			t.Fatalf("got  %s\nwant %s", got, want)
		}
	})
}
