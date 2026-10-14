package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/redact"

	"github.com/rs/zerolog"
)

func TestResponseRecorder(t *testing.T) {
	t.Run("body over the limit", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := newResponseRecorder(w, 4)
		for _, part := range []string{"ab", "cdef", "gh"} {
			if _, err := r.Write([]byte(part)); err != nil {
				t.Fatal(err)
			}
		}
		if string(r.Body()) != "abcd" || !r.BodyTruncated() || w.Body.String() != "abcdefgh" || r.StatusCode() != 200 {
			t.Fatalf("recorded %q truncated %v, sent %q, status %d", r.Body(), r.BodyTruncated(), w.Body, r.StatusCode())
		}
	})
	t.Run("no body recorded", func(t *testing.T) {
		r := newResponseRecorder(httptest.NewRecorder(), 0)
		if _, err := r.Write([]byte("a")); err != nil || r.Body() != nil || !r.BodyTruncated() {
			t.Fatalf("recorded %q truncated %v", r.Body(), r.BodyTruncated())
		}
	})
	t.Run("informational status first", func(t *testing.T) {
		r := newResponseRecorder(httptest.NewRecorder(), 0)
		r.WriteHeader(http.StatusEarlyHints)
		if r.Written() {
			t.Fatal("103 counted as the response")
		}
		r.WriteHeader(http.StatusCreated)
		r.WriteHeader(http.StatusTeapot)
		if !r.Written() || r.StatusCode() != http.StatusCreated {
			t.Fatalf("status %d", r.StatusCode())
		}
	})
	t.Run("response controller", func(t *testing.T) {
		w := httptest.NewRecorder()
		if err := http.NewResponseController(newResponseRecorder(w, 0)).Flush(); err != nil || !w.Flushed {
			t.Fatalf("flush: %v, flushed %v", err, w.Flushed)
		}
	})
}

func TestErrorLog(t *testing.T) {
	s := newLoggedServer(t, Config{Routes: testRoutes()})
	s.s.ErrorLog.Printf("http: TLS handshake error from %s: EOF", "192.0.2.1:1234")

	var line map[string]any
	if err := json.Unmarshal(s.logs.Bytes(), &line); err != nil {
		t.Fatalf("invalid log line %s: %v", s.logs.Bytes(), err)
	}
	if line["level"] != "error" || line["message"] != "http server error" || line["error"] != "http: TLS handshake error from 192.0.2.1:1234: EOF" ||
		line[logger.ErrorSeverityLogKey] != float64(1) {
		t.Fatalf("logged %v", line)
	}

	custom := log.New(&bytes.Buffer{}, "", 0)
	if s := newTestServer(t, Config{ErrorLog: custom, Routes: testRoutes()}); s.s.ErrorLog != custom {
		t.Fatal("Init replaced Config.ErrorLog")
	}
}

// marshalHeaders is how headers were logged before headerlog: clone, mask, json.Marshal. Header
// names are case-insensitive, so they match sensitive names with strings.EqualFold.
func marshalHeaders(headers http.Header, sensitive ...string) []byte {
	result := headers.Clone()
	for name := range result {
		for _, s := range slices.Concat(redact.CredentialHeaders, sensitive) {
			if strings.EqualFold(name, s) {
				result[name] = []string{"[*]"}
			}
		}
	}
	payload, _ := json.Marshal(result)
	return payload
}

// loggedHeaders returns the JSON logHeaders writes for h.
func loggedHeaders(t *testing.T, h http.Header, sensitive ...string) []byte {
	t.Helper()
	var out bytes.Buffer
	lg := zerolog.New(&out)
	e := lg.Info()
	logHeaders(e, "headers", h, math.MaxInt, newSensitiveFields(sensitive).headerValuesAppender())
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
		"configured":   {"X-Session": {"s"}, "x-session": {"t"}, "X-Sessions": {"kept"}},
		"nil values":   {"A": nil, "B": {}, "C": {"1", "2"}},
		"escaping":     {"X-Html": {"<a>&b"}, "X-Ctl": {"\x00\x01\b\f\n\r\t\x1f\x7f"}, "X-Sep": {"x y z"}},
		"unicode":      {"X-Utf8": {"é日本😀"}, "X-Bad": {"bad\xffutf8\xc3"}, "é": {"key"}},
		"over 32 keys": manyHeaders(40),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if got, want := loggedHeaders(t, h, "X-Session"), marshalHeaders(h, "X-Session"); !bytes.Equal(got, want) {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// FuzzLoggedHeaders covers masking: any spelling of a sensitive name must be masked.
func FuzzLoggedHeaders(f *testing.F) {
	f.Add("X-Test", "value")
	f.Add("authorization", "secret")
	f.Add("x-api-Key", "secret")
	f.Add("SET-COOKIE", "\xff< >&\x00")
	f.Fuzz(func(t *testing.T, name, value string) {
		h := http.Header{name: {value, name}, "Accept": {"*/*"}}
		if got, want := loggedHeaders(t, h, "X-Session"), marshalHeaders(h, "X-Session"); !bytes.Equal(got, want) {
			t.Fatalf("got  %s\nwant %s", got, want)
		}
	})
}
