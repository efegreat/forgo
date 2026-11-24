package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

type tenantReq struct {
	Tenant string `header:"X-Tenant"`
	Name   string `json:"name"`
}

func newTenantHandler() Handler[tenantReq, struct{}] {
	return func(context.Context, tenantReq) (struct{}, error) { return struct{}{}, nil }
}

// failingRoute returns a route whose handler fails with err.
func failingRoute(path string, err error) RouteConfig {
	return RouteConfig{Name: strings.TrimPrefix(path, "/"), Method: "GET", Path: path, Handler: HandlerAs[struct{}, struct{}](
		func() Handler[struct{}, struct{}] {
			return func(context.Context, struct{}) (struct{}, error) { return struct{}{}, err }
		})}
}

// TestBodyErrors checks the answers to bodies the default binder rejects.
func TestBodyErrors(t *testing.T) {
	routes := append(testRoutes(), RouteConfig{Name: "tenant", Method: "POST", Path: "/tenant", Handler: HandlerAs[tenantReq, struct{}](newTenantHandler)})
	s := newTestServer(t, Config{Routes: routes})
	for _, tc := range []struct {
		name, path, body string
		status           int
		code             string
	}{
		{"truncated", "/v1/items", `{"name":`, 400, "eof"},
		{"malformed", "/v1/items", `{"name" "pen"}`, 400, "json_syntax"},
		{"wrong type", "/v1/items", `{"name":1}`, 400, "json_type"},
		{"two documents", "/v1/items", `{"name":"a"} {"name":"b"}`, 400, "invalid_body"},
		{"field of another source", "/tenant", `{"Tenant":"t"}`, 400, "invalid_body"},
		{"empty body is absent", "/v1/items", ``, 400, "validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, envelope := serve(t, s, "POST", tc.path, tc.body)
			if w.Code != tc.status || errCode(envelope) != tc.code {
				t.Fatalf("%d %s, want %d %s", w.Code, w.Body, tc.status, tc.code)
			}
		})
	}
}

// TestHandlerErrorsAreNotBodyErrors checks that a handler error wrapping what a body error wraps stays a server
// error: the default mappers match only errors of the request body.
func TestHandlerErrorsAreNotBodyErrors(t *testing.T) {
	var syntax error = &json.SyntaxError{}
	if err := json.Unmarshal([]byte("{"), new(any)); err != nil {
		syntax = err
	}
	s := newTestServer(t, Config{Routes: []RouteConfig{
		failingRoute("/eof", fmt.Errorf("call upstream: %w", io.EOF)),
		failingRoute("/unexpected-eof", fmt.Errorf("read upstream: %w", io.ErrUnexpectedEOF)),
		failingRoute("/syntax", fmt.Errorf("decode upstream: %w", syntax)),
	}})
	for _, path := range []string{"/eof", "/unexpected-eof", "/syntax"} {
		if w, envelope := serve(t, s, "GET", path, ""); w.Code != 500 || errCode(envelope) != "internal" {
			t.Errorf("%s: %d %s, want 500 x", path, w.Code, w.Body)
		}
	}
}

func TestMaxBodyBytes(t *testing.T) {
	routes := func(override int64) []RouteConfig {
		return append(testRoutes(), RouteConfig{Name: "upload", Method: "POST", Path: "/upload", MaxBodyBytes: override,
			Handler: HandlerAs[itemReq, itemResp](newItemHandler)})
	}
	body := `{"name":"` + strings.Repeat("a", 64) + `"}`
	for _, tc := range []struct {
		name     string
		server   int64
		route    int64
		path     string
		status   int
		code     string
		wantBody string
	}{
		{"default limit", 0, 0, "/v1/items", 200, "", ""},
		{"server limit", 32, 0, "/v1/items", 413, "body_too_large", "request body larger than 32 bytes"},
		{"route inherits the limit", 32, 0, "/upload", 413, "body_too_large", ""},
		{"route raises the limit", 32, 1024, "/upload", 200, "", ""},
		{"route lowers the limit", 0, 32, "/upload", 413, "body_too_large", ""},
		{"no server limit", -1, 0, "/v1/items", 200, "", ""},
		{"no route limit", 32, -1, "/upload", 200, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBaseMiddlewares(t, func(t *testing.T, base bool) {
				cfg := Config{MaxBodyBytes: tc.server, Routes: routes(tc.route)}
				cfg.DisableBaseMiddlewares = !base
				s := newTestServer(t, cfg)
				w, envelope := serve(t, s, "POST", tc.path, body)
				if w.Code != tc.status || errCode(envelope) != tc.code {
					t.Fatalf("%d %s, want %d %s", w.Code, w.Body, tc.status, tc.code)
				}
				if msg, _ := envelope["errMsg"].(string); !strings.Contains(msg, tc.wantBody) {
					t.Fatalf("message %q, want %q", msg, tc.wantBody)
				}
			})
		})
	}
}

// skipBinder binds nothing, so handlers read the body themselves.
type skipBinder struct{}

func (skipBinder) Bind(*http.Request, any) error { return nil }

// TestMaxBodyBytesReadByHandler checks that a handler reading the body itself gets the same answer.
func TestMaxBodyBytesReadByHandler(t *testing.T) {
	read := HandlerAs[struct{}, struct{}](func() Handler[struct{}, struct{}] {
		return func(ctx context.Context, _ struct{}) (struct{}, error) {
			op, _ := operation.FromCtx(ctx)
			_, err := io.ReadAll(op.State.(*State).Request.Body)
			return struct{}{}, err
		}
	})
	s := newTestServer(t, Config{MaxBodyBytes: 8, Binder: skipBinder{},
		Routes: []RouteConfig{{Name: "raw", Method: "POST", Path: "/raw", Handler: read}}})
	if w, envelope := serve(t, s, "POST", "/raw", "0123456789"); w.Code != 413 || errCode(envelope) != "body_too_large" {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

// TestContextErrors checks that a handler error wrapping a context error is a light failure answered 408, and that
// an operation.Error around one keeps its own answer.
func TestContextErrors(t *testing.T) {
	errQuota := operation.Error{Code: "quota", Status: operation.StatusUnavailable, Message: "quota service down", Severity: operation.SeverityModerate}
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
		msg    string
		level  string
		logged string
	}{
		{"canceled", fmt.Errorf("query: %w", context.Canceled), 408, "canceled", "request canceled", "warn",
			"request canceled: query: context canceled"},
		{"deadline", context.DeadlineExceeded, 408, "deadline_exceeded", "deadline exceeded", "warn",
			"deadline exceeded: context deadline exceeded"},
		{"operation error", errQuota.Wrap(context.DeadlineExceeded), 503, "quota", "quota service down", "error",
			"quota service down: context deadline exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withBaseMiddlewares(t, func(t *testing.T, base bool) {
				cfg := Config{Routes: []RouteConfig{failingRoute("/fail", tc.err)}}
				cfg.DisableBaseMiddlewares = !base
				s := newLoggedServer(t, cfg)
				w := s.serve(httptest.NewRequest("GET", "/fail", nil))
				var envelope map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("response is not JSON: %q", w.Body)
				}
				if w.Code != tc.status || errCode(envelope) != tc.code || envelope["errMsg"] != tc.msg {
					t.Fatalf("%d %s, want %d %s %q", w.Code, w.Body, tc.status, tc.code, tc.msg)
				}
				if !base {
					return
				}
				failed := s.logLines(t)["operation failed"]
				if failed["level"] != tc.level || failed["err_code"] != tc.code || failed["error"] != tc.logged {
					t.Fatalf("logged %v", failed)
				}
			})
		})
	}
}

// TestClientDisconnect checks that a handler that returns its context's error once the client went away fails
// with a light canceled error, not a server error.
func TestClientDisconnect(t *testing.T) {
	started, canceled, release := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
	s := newTestServer(t, Config{Addr: freeAddr(t), Routes: blockingRoutes(started, canceled, release)})
	var logs lockedBuffer
	if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(&logs))); err != nil {
		t.Fatal(err)
	}
	served := startServer(t, s)
	t.Cleanup(func() {
		if err := s.Terminate(context.Background()); err != nil {
			t.Error(err)
		}
		waitServed(t, served)
	})

	ctx, cancel := context.WithCancel(context.Background())
	sent := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+s.cfg.Addr+"/block", nil)
		resp, err := testClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		sent <- err
	}()
	<-started
	cancel()
	if err := <-sent; !errors.Is(err, context.Canceled) {
		t.Fatalf("request returned %v, want it canceled", err)
	}
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not see the client go away")
	}

	var failed map[string]any
	for range 500 {
		for line := range strings.Lines(logs.String()) {
			var fields map[string]any
			if err := json.Unmarshal([]byte(line), &fields); err != nil {
				t.Fatalf("invalid log line %s: %v", line, err)
			}
			if fields["message"] == "operation failed" {
				failed = fields
			}
		}
		if failed != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failed["level"] != "warn" || failed["err_code"] != "canceled" || failed[logger.ErrorSeverityLogKey] != float64(operation.SeverityLight) ||
		failed[logger.ResponseStatusLogKey] != float64(http.StatusRequestTimeout) {
		t.Fatalf("logged %v", failed)
	}
}

// serveEnded serves r in process on a context that has already ended, as for a client that went away, with s
// logging to a buffer, and returns the response and the log lines.
func serveEnded(t *testing.T, s *server, r *http.Request) (*httptest.ResponseRecorder, []map[string]any) {
	t.Helper()
	var logs bytes.Buffer
	if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(&logs))); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r.WithContext(ctx))
	var lines []map[string]any
	for line := range bytes.Lines(logs.Bytes()) {
		var fields map[string]any
		if err := json.Unmarshal(line, &fields); err != nil {
			t.Fatalf("invalid log line %s: %v", line, err)
		}
		lines = append(lines, fields)
	}
	return w, lines
}

// assertCanceled fails unless w answers 408 canceled and the operation failed with a warning, with no line
// logged at error level.
func assertCanceled(t *testing.T, w *httptest.ResponseRecorder, lines []map[string]any) {
	t.Helper()
	if w.Code != http.StatusRequestTimeout || !strings.Contains(w.Body.String(), `"errCode":"canceled"`) {
		t.Fatalf("response %d %s", w.Code, w.Body)
	}
	var failed map[string]any
	for _, line := range lines {
		if line["level"] == "error" {
			t.Errorf("logged at error level: %v", line)
		}
		if line["message"] == "operation failed" {
			failed = line
		}
	}
	if failed["level"] != "warn" || failed["err_code"] != "canceled" {
		t.Fatalf("operation logged %v", failed)
	}
}
