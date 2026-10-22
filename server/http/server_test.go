package http

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

type itemReq struct {
	Name string `json:"name" validate:"required"`
}

type itemResp struct {
	Name string `json:"name"`
}

func newItemHandler() Handler[itemReq, itemResp] {
	return func(_ context.Context, req itemReq) (itemResp, error) {
		return itemResp(req), nil
	}
}

func newDenyMiddleware() operation.Middleware {
	return func(operation.Handler) operation.Handler {
		return func(*operation.Context) error {
			return operation.Error{Code: "denied", Status: operation.StatusPermissionDenied, Message: "denied"}
		}
	}
}

func testRoutes() []RouteConfig {
	return []RouteConfig{
		{Name: "health", Method: "GET", Path: "/health", Handler: EmptyHandler()},
		{Name: "items", Method: "POST", Path: "items", Version: "/v1/", Handler: HandlerAs[itemReq, itemResp](newItemHandler)},
		{Name: "denied", Method: "GET", Path: "/denied", Version: "v1", Handler: EmptyHandler(),
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(newDenyMiddleware)}},
	}
}

// newTestServer builds and initializes a server through bootstrap, like App.Run does.
func newTestServer(t testing.TB, cfg Config, providers ...any) *server {
	t.Helper()
	boot := forgo.NewBootstrap(nil, providers)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })

	s, err := cfg.New(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s.(*server)
}

func serve(t *testing.T, s *server, method, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	s.s.Handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	var envelope map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("%s %s: response is not JSON: %q", method, path, w.Body.String())
		}
	}
	return w, envelope
}

func TestRoutePattern(t *testing.T) {
	for _, tc := range []struct{ method, version, path, want string }{
		{"GET", "v1", "/users/{id}", "GET /v1/users/{id}"},
		{"GET", "/v1/", "users", "GET /v1/users"},
		{"GET", "", "/health", "GET /health"},
		{"", "v1", "/any", "/v1/any"},
		{"", "", "/", "/"},
	} {
		if got := routePattern(tc.method, tc.version, tc.path); got != tc.want {
			t.Errorf("routePattern(%q, %q, %q) = %q, want %q", tc.method, tc.version, tc.path, got, tc.want)
		}
	}
	if got := (RouteConfig{Version: "v2", Path: "/x"}).FullPath(); got != "/v2/x" {
		t.Errorf("FullPath = %q", got)
	}
}

func TestServerResponses(t *testing.T) {
	for _, base := range []bool{true, false} {
		cfg := Config{Routes: testRoutes()}
		cfg.DisableBaseMiddlewares = !base
		s := newTestServer(t, cfg)

		for _, tc := range []struct {
			method, path, body string
			status             int
			code               string
		}{
			{"GET", "/health", "", 200, ""},
			{"POST", "/v1/items", `{"name":"pen"}`, 200, ""},
			{"POST", "/v1/items", `{"name":`, 400, "eof"},
			{"GET", "/v1/denied", "", 403, "denied"},
			{"GET", "/nope", "", 404, "route_not_found"},
			{"DELETE", "/health", "", 404, "route_not_found"},
		} {
			w, envelope := serve(t, s, tc.method, tc.path, tc.body)
			if w.Code != tc.status {
				t.Errorf("base=%v %s %s: status %d, want %d (%s)", base, tc.method, tc.path, w.Code, tc.status, w.Body)
				continue
			}
			if code, _ := envelope["errCode"].(string); code != tc.code {
				t.Errorf("base=%v %s %s: errCode %q, want %q", base, tc.method, tc.path, code, tc.code)
			}
		}
	}
}

func TestEmptyHandlerOmitsResult(t *testing.T) {
	s := newTestServer(t, Config{Routes: testRoutes()})
	_, envelope := serve(t, s, "GET", "/health", "")
	if _, ok := envelope["result"]; ok {
		t.Fatalf("empty handler response has a result: %v", envelope)
	}
}

func TestHealthHandler(t *testing.T) {
	routes := []RouteConfig{{Name: "ready", Method: "GET", Path: "/ready", Handler: HealthHandler()}}
	for _, base := range []bool{true, false} {
		ts := &forgo.TermSig{}
		cfg := Config{Routes: routes}
		cfg.Term.TermSig = ts
		cfg.DisableBaseMiddlewares = !base
		s := newTestServer(t, cfg)
		unsignaled := newTestServer(t, Config{Routes: routes})

		w, envelope := serve(t, s, "GET", "/ready", "")
		if _, ok := envelope["result"]; w.Code != 200 || ok {
			t.Fatalf("base=%v before signal: status %d, body %s", base, w.Code, w.Body)
		}

		ts.Signal()
		w, envelope = serve(t, s, "GET", "/ready", "")
		if code, _ := envelope["errCode"].(string); w.Code != 503 || code != "terminating" {
			t.Errorf("base=%v after signal: status %d, body %s", base, w.Code, w.Body)
		}
		if w, _ := serve(t, unsignaled, "GET", "/ready", ""); w.Code != 200 {
			t.Errorf("base=%v without TermSig: status %d, body %s", base, w.Code, w.Body)
		}
	}
}

func TestRootRouteReplacesNotFound(t *testing.T) {
	routes := append(testRoutes(), RouteConfig{Name: "root", Path: "/", Handler: EmptyHandler()})
	s := newTestServer(t, Config{Routes: routes})
	if w, _ := serve(t, s, "GET", "/anything", ""); w.Code != 200 {
		t.Fatalf("root route did not handle unmatched path: %d", w.Code)
	}
}

func TestServeTLS(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	s := newTestServer(t, Config{
		Addr:      addr,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{selfSignedCert(t)}},
		Routes:    testRoutes(),
	})
	done := make(chan error, 1)
	go func() { done <- s.Serve() }()
	t.Cleanup(func() {
		_ = s.Terminate(context.Background())
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	var resp *http.Response
	for range 50 {
		if resp, err = client.Get("https://" + addr + "/health"); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.TLS == nil {
		t.Fatalf("status %d, tls %v", resp.StatusCode, resp.TLS != nil)
	}
}

func selfSignedCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

type routeArgKey struct{}

// TestRouteArgsAreIsolated checks that route args are visible to every request, while a write to
// ctx.Args stays in its own request and never reaches the route config.
func TestRouteArgsAreIsolated(t *testing.T) {
	var seen []any
	overwrite := func() operation.Middleware {
		return func(next operation.Handler) operation.Handler {
			return func(ctx *operation.Context) error {
				seen = append(seen, ctx.Value(routeArgKey{}))
				ctx.Args.Set(routeArgKey{}, "overwritten")
				return next(ctx)
			}
		}
	}
	args := map[any]any{routeArgKey{}: "route"}
	s := newTestServer(t, Config{Routes: []RouteConfig{{
		Name: "args", Method: "GET", Path: "/args", Handler: EmptyHandler(), Args: args,
		Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(overwrite)},
	}}})

	serve(t, s, "GET", "/args", "")
	serve(t, s, "GET", "/args", "")

	if len(seen) != 2 || seen[0] != "route" || seen[1] != "route" {
		t.Fatalf("requests saw %v, want [route route]", seen)
	}
	if args[routeArgKey{}] != "route" {
		t.Fatalf("route config args changed to %v", args[routeArgKey{}])
	}
}

// TestRecorderDisabledByRouteArgs checks that ArgKeyRecorderDisabled turns the Recorder off, and that a
// key of another type with the same value does not.
func TestRecorderDisabledByRouteArgs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   map[any]any
		logged bool
	}{
		{"disabled", map[any]any{ArgKeyRecorderDisabled: true}, false},
		{"enabled", map[any]any{ArgKeyRecorderDisabled: false}, true},
		{"other key type", map[any]any{uint8(ArgKeyRecorderDisabled): true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLoggedServer(t, Config{Routes: []RouteConfig{{
				Name: "quiet", Method: "GET", Path: "/quiet", Handler: EmptyHandler(), Args: tc.args,
			}}})
			s.logs.Reset()
			s.serve(httptest.NewRequest("GET", "/quiet", nil))
			if logged := s.logLines(t)["operation completed"] != nil; logged != tc.logged {
				t.Fatalf("operation logged %v, want %v:\n%s", logged, tc.logged, &s.logs)
			}
		})
	}
}
