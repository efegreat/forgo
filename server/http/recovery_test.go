package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

// panicRoute returns a route whose middleware panics with fn.
func panicRoute(name string, fn func()) RouteConfig {
	return RouteConfig{
		Name: name, Method: "GET", Path: "/" + name, Handler: EmptyHandler(),
		Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(func() operation.Middleware {
			return func(operation.Handler) operation.Handler {
				return func(*operation.Context) error {
					fn()
					return nil
				}
			}
		})},
	}
}

// TestPanicResponses checks that a panic is answered through the ErrorMapper and that the logged
// cause keeps the stack, whatever the panic value.
func TestPanicResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		panic      func()
		wantStatus int
		wantCode   string
		wantErr    string
	}{
		{"string", func() { panic("boom") }, 500, "x", "an error occurred: panic: boom"},
		{"runtime error", func() {
			var m map[string]int
			m["a"] = 1
		}, 500, "x", "an error occurred: panic: assignment to entry in nil map"},
		{"operation error", func() {
			panic(operation.Error{Code: "gone", Status: operation.StatusNotFound, Message: "gone"})
		}, 404, "gone", "gone: panic: gone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLoggedServer(t, Config{Routes: []RouteConfig{panicRoute("p", tc.panic)}})
			w := s.serve(httptest.NewRequest("GET", "/p", nil))

			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), `"errCode":"`+tc.wantCode+`"`) {
				t.Fatalf("response %d %s, want %d with errCode %s", w.Code, w.Body, tc.wantStatus, tc.wantCode)
			}
			if strings.Contains(w.Body.String(), "goroutine") {
				t.Fatalf("response leaks the stack: %s", w.Body)
			}
			failed := s.logLines(t)["operation failed"]
			logged, _ := failed["error"].(string)
			if !strings.HasPrefix(logged, tc.wantErr) || !strings.Contains(logged, "goroutine") {
				t.Fatalf("logged error %q, want %q with the stack", logged, tc.wantErr)
			}
		})
	}
}

func TestPanicAbortHandler(t *testing.T) {
	s := newLoggedServer(t, Config{Routes: []RouteConfig{panicRoute("abort", func() { panic(http.ErrAbortHandler) })}})
	defer func() {
		if r := recover(); r != http.ErrAbortHandler {
			t.Fatalf("recovered %v, want http.ErrAbortHandler re-panicked for net/http", r)
		}
	}()
	s.serve(httptest.NewRequest("GET", "/abort", nil))
	t.Fatal("ErrAbortHandler was not re-panicked")
}

// TestRequestID checks that a well-formed X-Request-ID is kept and anything else is replaced.
func TestRequestID(t *testing.T) {
	s := newTestServer(t, Config{Routes: testRoutes()})
	for _, tc := range []struct {
		name string
		in   string
		keep bool
	}{
		{"none", "", false},
		{"well-formed", "trace-0199c3a4", true},
		{"oversized", strings.Repeat("a", 129), false},
		{"space", "two words", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/health", nil)
			if tc.in != "" {
				r.Header.Set(ReqIDHeader, tc.in)
			}
			w := httptest.NewRecorder()
			s.s.Handler.ServeHTTP(w, r)

			got := w.Header().Get(ReqIDHeader)
			if got == "" || (got == tc.in) != tc.keep {
				t.Fatalf("X-Request-ID %q for %q, want kept %v", got, tc.in, tc.keep)
			}
		})
	}
}
