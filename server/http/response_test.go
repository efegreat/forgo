package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

var errDeclined = operation.Error{Code: "declined", Status: operation.StatusPreconditionFailed, Message: "card declined"}

// writingRoute returns a route whose handler writes the response itself with write, then returns err.
func writingRoute(path string, write func(w http.ResponseWriter, r *http.Request), err error) RouteConfig {
	return RouteConfig{Name: strings.TrimPrefix(path, "/"), Method: "GET", Path: path, Handler: HandlerAs[struct{}, itemResp](
		func() Handler[struct{}, itemResp] {
			return func(ctx context.Context, _ struct{}) (itemResp, error) {
				op, _ := operation.FromCtx(ctx)
				st := op.State.(*State)
				write(st.Writer, st.Request)
				return itemResp{Name: "ignored"}, err
			}
		})}
}

// TestPanicAfterResponse checks that a panic after the response was written keeps that response, and is logged
// as a failure.
func TestPanicAfterResponse(t *testing.T) {
	panicAfter := forgo.MiddlewareAs(func() operation.Middleware {
		return func(next operation.Handler) operation.Handler {
			return func(ctx *operation.Context) error {
				_ = next(ctx)
				panic("after the response")
			}
		}
	})
	cfg := Config{Routes: testRoutes()}
	cfg.Middlewares = []forgo.MiddlewareFactory{panicAfter}
	s := newLoggedServer(t, cfg)

	w := s.serve(httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 || strings.Count(w.Body.String(), "reqId") != 1 || strings.Contains(w.Body.String(), "errCode") {
		t.Fatalf("response %d %s, want the health response alone", w.Code, w.Body)
	}
	failed := s.logLines(t)["operation failed"]
	if logged, _ := failed["error"].(string); !strings.Contains(logged, "panic: after the response") || !strings.Contains(logged, "goroutine") {
		t.Fatalf("logged error %q, want the panic with its stack", logged)
	}
}

// TestHandlerWritesResponse checks that a handler that writes the response itself keeps it, whether it then
// returns a result or an error, and that a handler that only sets headers still gets the envelope.
func TestHandlerWritesResponse(t *testing.T) {
	redirect := func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/elsewhere", http.StatusFound) }
	csv := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("a,b\n"))
	}
	cache := func(w http.ResponseWriter, _ *http.Request) { w.Header().Set("Cache-Control", "max-age=60") }
	routes := []RouteConfig{
		writingRoute("/redirect", redirect, nil),
		writingRoute("/csv", csv, nil),
		writingRoute("/csv-then-error", csv, errDeclined),
		writingRoute("/cache", cache, nil),
	}
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		cfg := Config{Routes: routes}
		cfg.DisableBaseMiddlewares = !base
		s := newTestServer(t, cfg)
		for _, tc := range []struct {
			path, header, value string
			status              int
			body                string
		}{
			{"/redirect", "Location", "/elsewhere", http.StatusFound, `<a href="/elsewhere">Found</a>.` + "\n\n"},
			{"/csv", "Content-Type", "text/csv", 200, "a,b\n"},
			{"/csv-then-error", "Content-Type", "text/csv", 200, "a,b\n"},
			{"/cache", "Cache-Control", "max-age=60", 200, ""},
		} {
			w := httptest.NewRecorder()
			s.s.Handler.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
			if w.Code != tc.status || w.Header().Get(tc.header) != tc.value {
				t.Errorf("%s: %d %s %q", tc.path, w.Code, tc.header, w.Header().Get(tc.header))
			}
			if tc.body != "" && w.Body.String() != tc.body {
				t.Errorf("%s: body %q, want %q", tc.path, w.Body, tc.body)
			}
			if tc.body == "" && !strings.Contains(w.Body.String(), `"name":"ignored"`) {
				t.Errorf("%s: body %q, want the envelope", tc.path, w.Body)
			}
		}
	})

	s := newLoggedServer(t, Config{Routes: routes})
	s.serve(httptest.NewRequest("GET", "/csv-then-error", nil))
	if failed := s.logLines(t)["operation failed"]; failed["err_code"] != "declined" {
		t.Fatalf("a written response hid the handler's error: %v", failed)
	}
}

// TestErrorContextIsLogged checks that the operation fails with the handler's error itself, with what is wrapped
// around its operation.Error, while the client gets the operation.Error alone.
func TestErrorContextIsLogged(t *testing.T) {
	wrapped := fmt.Errorf("charge order 42: %w", errDeclined.Wrap(errors.New("issuer said no")))
	s := newLoggedServer(t, Config{Routes: []RouteConfig{failingRoute("/charge", wrapped)}})

	w := s.serve(httptest.NewRequest("GET", "/charge", nil))
	if w.Code != http.StatusPreconditionFailed || !strings.Contains(w.Body.String(), `"errMsg":"card declined"`) {
		t.Fatalf("response %d %s", w.Code, w.Body)
	}
	failed := s.logLines(t)["operation failed"]
	if failed["error"] != "charge order 42: card declined: issuer said no" || failed["err_code"] != "declined" {
		t.Fatalf("logged %v", failed)
	}
}

// TestMappedErrorKeepsCause checks that the log keeps the original error when a mapper leaves it out of the
// error it returns.
func TestMappedErrorKeepsCause(t *testing.T) {
	errGone := errors.New("row gone")
	mapper := operation.NewErrorMapper(func(err error) (operation.Error, bool) {
		if errors.Is(err, errGone) {
			return operation.Error{Code: "gone", Status: operation.StatusNotFound, Message: "gone"}, true
		}
		return operation.Error{}, false
	})
	s := newLoggedServer(t, Config{ErrorMapper: mapper, Routes: []RouteConfig{failingRoute("/gone", errGone)}})

	if w := s.serve(httptest.NewRequest("GET", "/gone", nil)); w.Code != http.StatusNotFound {
		t.Fatalf("response %d %s", w.Code, w.Body)
	}
	if failed := s.logLines(t)["operation failed"]; failed["error"] != "gone: row gone" || failed["err_code"] != "gone" {
		t.Fatalf("logged %v", failed)
	}
}
