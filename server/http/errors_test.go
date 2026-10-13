package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/efegreat/forgo/operation"
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
		if w, envelope := serve(t, s, "GET", path, ""); w.Code != 500 || errCode(envelope) != "x" {
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
