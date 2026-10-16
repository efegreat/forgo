package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/internal/jwttest"
	"github.com/efegreat/forgo/jwtauth"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

type benchArgKey struct{}

// benchCtx carries a logger that discards output, like the server context App passes to Init.
func benchCtx() context.Context {
	return logger.NewCtx(context.Background(), zerolog.New(io.Discard))
}

// benchHeaders returns typical client request headers, two of them sensitive.
func benchHeaders() http.Header {
	return http.Header{
		"Accept":          {"application/json"},
		"Accept-Encoding": {"gzip, deflate, br"},
		"Authorization":   {"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"},
		"Content-Type":    {"application/json"},
		"Cookie":          {"session=abc123; theme=dark"},
		"User-Agent":      {"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"},
	}
}

// BenchmarkServeHTTP measures a request through the full handler with base middlewares and request logging.
// Each iteration also pays for building the request and the recorder.
func BenchmarkServeHTTP(b *testing.B) {
	routes := append(testRoutes(), RouteConfig{
		Name: "args", Method: "GET", Path: "/args", Handler: EmptyHandler(),
		Args: map[any]any{benchArgKey{}: "tenant", ArgKeyRecorderDisabled: false},
	})
	h := newTestServer(b, Config{Routes: routes}).s.Handler
	ctx := benchCtx()

	cases := []struct {
		name, method, path, body string
	}{
		{"get", "GET", "/health", ""},
		{"get_args", "GET", "/args", ""},
		{"post", "POST", "/v1/items", `{"name":"widget"}`},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				r := httptest.NewRequestWithContext(ctx, c.method, c.path, strings.NewReader(c.body))
				r.Header = benchHeaders()
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
			}
		})
	}
}

// BenchmarkHeaderLogFields measures the request and response log fields of the Recorder.
func BenchmarkHeaderLogFields(b *testing.B) {
	r := httptest.NewRequest("GET", "/health", nil)
	r.Header = benchHeaders()
	rec := newResponseRecorder(httptest.NewRecorder(), ResponseRecorderBodyLimit)
	rec.Header().Set("Content-Type", "application/json; charset=utf-8")
	rec.Header().Set(ReqIDHeader, "01a11d60-2887-77e7-9adc-75ddcaecee37")
	rec.Header().Set("Set-Cookie", "session=abc123; Path=/; HttpOnly")
	rec.WriteHeader(http.StatusOK)

	opCtx := &operation.Context{Context: context.Background(), State: &State{Request: r, Writer: rec}}
	reqFields := RequestLogFields(RequestRecorderBodyLimit, RequestRecorderHeaderLimit, nil)
	respFields := ResponseLogFields(ResponseRecorderHeaderLimit, nil)
	lg := zerolog.New(io.Discard)

	b.ReportAllocs()
	for b.Loop() {
		e := lg.Info()
		reqFields(opCtx, e)
		e.Msg("operation started")

		e = lg.Info()
		respFields(opCtx, e)
		e.Msg("operation completed")
	}
}

// BenchmarkMiddlewares measures each HTTP middleware alone around a no-op handler.
// Each iteration also pays for the operation context and the response recorder.
func BenchmarkMiddlewares(b *testing.B) {
	k := jwttest.Keys()
	bearer := func(alg string, key any) func(*http.Request) {
		token := jwttest.Sign(b, map[string]any{"alg": alg}, liveClaims(nil), key)
		return func(r *http.Request) { r.Header.Set(authorizationHeader, "Bearer "+token) }
	}
	origin := func(r *http.Request) { r.Header.Set(originHeader, "https://app.example") }

	cases := []struct {
		name    string
		method  string
		new     func() (operation.Middleware, error)
		prepare func(*http.Request)
	}{
		{"baseline", "GET", func() (operation.Middleware, error) {
			return func(next operation.Handler) operation.Handler { return next }, nil
		}, func(*http.Request) {}},
		{"cors_request", "GET", CORS(CORSConfig{AllowOrigins: []string{"https://app.example"}}), origin},
		{"cors_preflight", "OPTIONS", CORS(CORSConfig{}), func(r *http.Request) {
			origin(r)
			r.Header.Set(requestMethodHeader, "POST")
		}},
		{"rate_limit", "GET", RateLimit(RateLimitConfig{Limit: 1 << 30, Period: time.Second}), func(*http.Request) {}},
		{"jwt_hs256", "GET", JWTAuth(JWTConfig{Keys: jwtauth.Keys{{Key: k.Secret}}}), bearer("HS256", k.Secret)},
		{"jwt_es256", "GET", JWTAuth(JWTConfig{Keys: jwtauth.Keys{{Key: &k.P256.PublicKey}}}), bearer("ES256", k.P256)},
		{"jwt_rs256", "GET", JWTAuth(JWTConfig{Keys: jwtauth.Keys{{Key: &k.RSA.PublicKey}}}), bearer("RS256", k.RSA)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			mw, err := c.new()
			if err != nil {
				b.Fatal(err)
			}
			h := mw(func(*operation.Context) error { return nil })
			r := httptest.NewRequestWithContext(benchCtx(), c.method, "/health", nil)
			c.prepare(r)
			b.ReportAllocs()
			for b.Loop() {
				ctx := &operation.Context{
					Context: r.Context(),
					Args:    new(types.Map[any, any]),
					State:   &State{Request: r, Writer: newResponseRecorder(httptest.NewRecorder(), 0)},
				}
				if err := h(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
