package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
)

type whoAmIResp struct {
	Subject string `json:"subject"`
}

// newWhoAmIHandler answers with the subject JWTAuth verified, or "anonymous".
func newWhoAmIHandler() Handler[struct{}, whoAmIResp] {
	return func(ctx context.Context, _ struct{}) (whoAmIResp, error) {
		// Claims survive contexts derived from the operation.
		derived, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if c, ok := JWTClaimsFromCtx(derived); ok {
			return whoAmIResp{Subject: c.Subject}, nil
		}
		return whoAmIResp{Subject: "anonymous"}, nil
	}
}

// withBaseMiddlewares runs fn with base middlewares enabled and disabled: the middlewares must
// behave the same either way.
func withBaseMiddlewares(t *testing.T, fn func(t *testing.T, base bool)) {
	for _, base := range []bool{true, false} {
		t.Run(fmt.Sprintf("base=%v", base), func(t *testing.T) { fn(t, base) })
	}
}

func do(t *testing.T, s *Server, r *http.Request) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	s.s.Handler.ServeHTTP(w, r)
	var envelope map[string]any
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("%s %s: response is not JSON: %q", r.Method, r.URL, w.Body.String())
		}
	}
	return w, envelope
}

func errCode(envelope map[string]any) string {
	code, _ := envelope["errCode"].(string)
	return code
}

// newServerErr builds a server through bootstrap and returns the error of ServerFactory.New.
func newServerErr(t *testing.T, cfg Config, providers ...any) error {
	t.Helper()
	boot := forgo.NewBootstrap(nil, providers)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	_, err = cfg.New(r)
	return err
}

func serverMiddlewares(cfg Config, base bool, factories ...forgo.MiddlewareFactory) Config {
	cfg.Middlewares = factories
	cfg.DisableBaseMiddlewares = !base
	return cfg
}

func preflight(path, origin string) *http.Request {
	r := httptest.NewRequest(http.MethodOptions, path, nil)
	r.Header.Set(originHeader, origin)
	r.Header.Set(requestMethodHeader, http.MethodPost)
	r.Header.Set(requestHeadersHeader, "content-type, authorization")
	return r
}

func withOrigin(r *http.Request, origin string) *http.Request {
	r.Header.Set(originHeader, origin)
	return r
}

func TestCORSDefaults(t *testing.T) {
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		s := newTestServer(t, serverMiddlewares(Config{Routes: testRoutes()}, base, forgo.MiddlewareAs(CORS(CORSConfig{}))))

		w, _ := do(t, s, preflight("/v1/items", "https://app.example"))
		if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
			t.Fatalf("preflight: %d %q", w.Code, w.Body)
		}
		for header, want := range map[string]string{
			allowOriginHeader:      "*",
			allowMethodsHeader:     "GET, HEAD, PUT, PATCH, POST, DELETE",
			allowHeadersHeader:     "content-type, authorization",
			maxAgeHeader:           "600",
			allowCredentialsHeader: "",
			varyHeader:             preflightVary,
		} {
			if got := w.Header().Get(header); got != want {
				t.Errorf("preflight %s = %q, want %q", header, got, want)
			}
		}

		w, _ = do(t, s, withOrigin(httptest.NewRequest("POST", "/v1/items", strings.NewReader(`{"name":"pen"}`)), "https://app.example"))
		if w.Code != http.StatusOK {
			t.Fatalf("request: %d %s", w.Code, w.Body)
		}
		if got := w.Header().Get(allowOriginHeader); got != "*" {
			t.Errorf("request %s = %q", allowOriginHeader, got)
		}
		if got, want := w.Header().Get(exposeHeadersHeader), "X-Request-ID, Retry-After, Ratelimit-Limit, Ratelimit-Remaining, Ratelimit-Reset"; got != want {
			t.Errorf("request %s = %q, want %q", exposeHeadersHeader, got, want)
		}
		if vary := w.Header().Values(varyHeader); len(vary) != 0 {
			t.Errorf("wildcard response varies on %v", vary)
		}

		// Error responses, from routes and from the not-found fallback, carry the headers too,
		// so scripts can read them.
		for path, status := range map[string]int{"/v1/denied": http.StatusForbidden, "/nope": http.StatusNotFound} {
			w, _ = do(t, s, withOrigin(httptest.NewRequest("GET", path, nil), "https://app.example"))
			if w.Code != status || w.Header().Get(allowOriginHeader) != "*" {
				t.Errorf("GET %s: %d, %s %q", path, w.Code, allowOriginHeader, w.Header().Get(allowOriginHeader))
			}
		}

		w, _ = do(t, s, httptest.NewRequest("GET", "/health", nil))
		if w.Code != http.StatusOK || w.Header().Get(allowOriginHeader) != "" {
			t.Errorf("same-origin request: %d, %s %q", w.Code, allowOriginHeader, w.Header().Get(allowOriginHeader))
		}
	})
}

func TestCORSOrigins(t *testing.T) {
	cors := CORSConfig{
		AllowOrigins:     []string{"https://app.example.com", " https://*.Example.org ", "http://localhost:*"},
		AllowMethods:     []string{"get", "post"},
		AllowHeaders:     []string{"Content-Type", "X-Tenant"},
		ExposeHeaders:    []string{"X-Total"},
		AllowCredentials: true,
		MaxAge:           -1,
	}
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		s := newTestServer(t, serverMiddlewares(Config{Routes: testRoutes()}, base, forgo.MiddlewareAs(CORS(cors))))

		for origin, allowed := range map[string]bool{
			"https://app.example.com":       true,
			"HTTPS://APP.EXAMPLE.COM":       true,
			"https://evil.example.com":      false,
			"https://app.example.com.evil":  false,
			"https://a.example.org":         true,
			"https://a.b.example.org":       true,
			"https://example.org":           false,
			"https://.example.org":          false,
			"http://a.example.org":          false,
			"https://a.example.org.evil.io": false,
			"http://localhost:3000":         true,
			"http://localhost":              false,
			"null":                          false,
		} {
			w, _ := do(t, s, withOrigin(httptest.NewRequest("GET", "/health", nil), origin))
			if w.Code != http.StatusOK {
				t.Fatalf("%s: status %d", origin, w.Code)
			}
			if got := w.Header().Values(varyHeader); len(got) != 1 || got[0] != originHeader {
				t.Errorf("%s: Vary %v", origin, got)
			}
			want := map[string]string{allowOriginHeader: "", allowCredentialsHeader: "", exposeHeadersHeader: ""}
			if allowed {
				want = map[string]string{allowOriginHeader: origin, allowCredentialsHeader: "true", exposeHeadersHeader: "X-Total"}
			}
			for header, v := range want {
				if got := w.Header().Get(header); got != v {
					t.Errorf("%s: %s = %q, want %q", origin, header, got, v)
				}
			}

			w, _ = do(t, s, preflight("/v1/items", origin))
			want = map[string]string{allowOriginHeader: "", allowMethodsHeader: "", allowHeadersHeader: "", maxAgeHeader: ""}
			if allowed {
				want = map[string]string{
					allowOriginHeader:  origin,
					allowMethodsHeader: "GET, POST",
					allowHeadersHeader: "Content-Type, X-Tenant",
					maxAgeHeader:       "0",
				}
			}
			if w.Code != http.StatusNoContent {
				t.Errorf("%s: preflight status %d", origin, w.Code)
			}
			for header, v := range want {
				if got := w.Header().Get(header); got != v {
					t.Errorf("%s: preflight %s = %q, want %q", origin, header, got, v)
				}
			}
		}

		// Responses depend on Origin even when a request has none, so caches keep them apart.
		w, _ := do(t, s, httptest.NewRequest("GET", "/health", nil))
		if got := w.Header().Get(varyHeader); got != originHeader {
			t.Errorf("no origin: Vary %q", got)
		}
	})
}

func TestCORSOriginFunc(t *testing.T) {
	cors := CORSConfig{
		AllowOrigins:    []string{"https://static.example"},
		AllowOriginFunc: func(origin string) bool { return origin == "https://tenant.example" },
	}
	s := newTestServer(t, serverMiddlewares(Config{Routes: testRoutes()}, true, forgo.MiddlewareAs(CORS(cors))))
	for origin, want := range map[string]string{
		"https://static.example": "https://static.example",
		"https://tenant.example": "https://tenant.example",
		"https://other.example":  "",
	} {
		w, _ := do(t, s, withOrigin(httptest.NewRequest("GET", "/health", nil), origin))
		if got := w.Header().Get(allowOriginHeader); got != want {
			t.Errorf("%s: %s = %q, want %q", origin, allowOriginHeader, got, want)
		}
	}

	// A function alone does not fall back to "*".
	s = newTestServer(t, serverMiddlewares(Config{Routes: testRoutes()}, true, forgo.MiddlewareAs(CORS(CORSConfig{AllowOriginFunc: cors.AllowOriginFunc}))))
	if w, _ := do(t, s, withOrigin(httptest.NewRequest("GET", "/health", nil), "https://other.example")); w.Header().Get(allowOriginHeader) != "" {
		t.Errorf("function-only policy allowed another origin")
	}
}

func TestCORSConfigErrors(t *testing.T) {
	for name, cors := range map[string]CORSConfig{
		"credentials with default origin": {AllowCredentials: true},
		"credentials with *":              {AllowOrigins: []string{"https://a.example", "*"}, AllowCredentials: true},
		"origin with path":                {AllowOrigins: []string{"https://app.example/"}},
		"origin without scheme":           {AllowOrigins: []string{"app.example"}},
		"two wildcards":                   {AllowOrigins: []string{"https://*.*.example"}},
		"wildcard scheme":                 {AllowOrigins: []string{"*://app.example"}},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := serverMiddlewares(Config{Routes: testRoutes()}, true, forgo.MiddlewareAs(CORS(cors)))
			if err := newServerErr(t, cfg); err == nil || !strings.Contains(err.Error(), "http: cors:") {
				t.Fatalf("err %v", err)
			}
		})
	}
}

func rateLimitedRoutes(limit forgo.MiddlewareFactory) []RouteConfig {
	return []RouteConfig{
		{Name: "a", Method: "GET", Path: "/a", Handler: EmptyHandler(), Middlewares: []forgo.MiddlewareFactory{limit}},
		{Name: "b", Method: "GET", Path: "/b", Handler: EmptyHandler(), Middlewares: []forgo.MiddlewareFactory{limit}},
	}
}

func get(path, remote string) *http.Request {
	r := httptest.NewRequest("GET", path, nil)
	r.RemoteAddr = remote
	return r
}

func TestRateLimit(t *testing.T) {
	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		limit := forgo.MiddlewareAs(RateLimit(RateLimitConfig{Limit: 2, Period: time.Minute}))
		cfg := Config{Routes: rateLimitedRoutes(limit)}
		cfg.DisableBaseMiddlewares = !base
		s := newTestServer(t, cfg)

		for i, want := range []struct {
			status                       int
			remaining, reset, retryAfter string
		}{
			{http.StatusOK, "1", "30", ""},
			{http.StatusOK, "0", "60", ""},
			{http.StatusTooManyRequests, "0", "60", "30"},
		} {
			w, envelope := do(t, s, get("/a", "203.0.113.1:1000"))
			if w.Code != want.status {
				t.Fatalf("request %d: status %d, want %d", i, w.Code, want.status)
			}
			h := w.Header()
			if h.Get(rateLimitLimitHeader) != "2" || h.Get(rateLimitRemainingHeader) != want.remaining ||
				h.Get(rateLimitResetHeader) != want.reset || h.Get(retryAfterHeader) != want.retryAfter {
				t.Fatalf("request %d: headers %v", i, h)
			}
			if want.status == http.StatusTooManyRequests && errCode(envelope) != ErrRateLimited.Code {
				t.Fatalf("request %d: errCode %q", i, errCode(envelope))
			}
		}

		// Another client, another port of the same client, and another route each count apart.
		for _, r := range []*http.Request{get("/a", "203.0.113.2:1000"), get("/b", "203.0.113.1:1000")} {
			if w, _ := do(t, s, r); w.Code != http.StatusOK {
				t.Fatalf("%s from %s: status %d", r.URL.Path, r.RemoteAddr, w.Code)
			}
		}
		if w, _ := do(t, s, get("/a", "203.0.113.1:2000")); w.Code != http.StatusTooManyRequests {
			t.Fatalf("other port of a limited client: status %d", w.Code)
		}
	})
}

func TestRateLimitServerWide(t *testing.T) {
	cfg := serverMiddlewares(Config{Routes: testRoutes()}, true, forgo.MiddlewareAs(RateLimit(RateLimitConfig{Limit: 1})))
	s := newTestServer(t, cfg)
	if w, _ := do(t, s, get("/health", "203.0.113.1:1")); w.Code != http.StatusOK {
		t.Fatalf("first: %d", w.Code)
	}
	if w, envelope := do(t, s, get("/nope", "203.0.113.1:1")); w.Code != http.StatusTooManyRequests || errCode(envelope) != "rate_limited" {
		t.Fatalf("second, on another path: %d %v", w.Code, envelope)
	}
}

func TestRateLimitKeys(t *testing.T) {
	proxies := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	behindProxy := func(client string) *http.Request {
		r := get("/a", "10.1.2.3:443")
		r.Header.Set(forwardedForHeader, "6.6.6.6, "+client)
		return r
	}
	s := newTestServer(t, Config{Routes: rateLimitedRoutes(forgo.MiddlewareAs(RateLimit(RateLimitConfig{Limit: 1, TrustedProxies: proxies})))})
	for i, tc := range []struct {
		r      *http.Request
		status int
	}{
		{behindProxy("198.51.100.1"), http.StatusOK},
		{behindProxy("198.51.100.2"), http.StatusOK},
		{behindProxy("198.51.100.1"), http.StatusTooManyRequests},
		{get("/a", "[2001:db8::1]:1"), http.StatusOK},
		{get("/a", "[2001:db8::2]:1"), http.StatusTooManyRequests}, // same /64
		{get("/a", "[2001:db8:0:1::1]:1"), http.StatusOK},
	} {
		if w, _ := do(t, s, tc.r); w.Code != tc.status {
			t.Fatalf("request %d: status %d, want %d", i, w.Code, tc.status)
		}
	}

	keyErr := operation.Error{Code: "no_tenant", Status: operation.StatusInvalidArgument, Message: "missing tenant"}
	byTenant := RateLimitConfig{Limit: 1, Key: func(ctx *operation.Context) (string, error) {
		switch tenant := ctx.State.(*State).Request.Header.Get("X-Tenant"); tenant {
		case "":
			return "", keyErr
		case "internal":
			return "", nil
		default:
			return tenant, nil
		}
	}}
	s = newTestServer(t, Config{Routes: rateLimitedRoutes(forgo.MiddlewareAs(RateLimit(byTenant)))})
	withTenant := func(tenant string) *http.Request {
		r := get("/a", "203.0.113.1:1")
		if tenant != "" {
			r.Header.Set("X-Tenant", tenant)
		}
		return r
	}
	for i, tc := range []struct {
		tenant string
		status int
		code   string
	}{
		{"acme", http.StatusOK, ""},
		{"acme", http.StatusTooManyRequests, "rate_limited"},
		{"globex", http.StatusOK, ""},
		{"internal", http.StatusOK, ""},
		{"internal", http.StatusOK, ""},
		{"", http.StatusBadRequest, "no_tenant"},
	} {
		w, envelope := do(t, s, withTenant(tc.tenant))
		if w.Code != tc.status || errCode(envelope) != tc.code {
			t.Fatalf("request %d: %d %q, want %d %q", i, w.Code, errCode(envelope), tc.status, tc.code)
		}
		if tc.tenant == "internal" && w.Header().Get(rateLimitLimitHeader) != "" {
			t.Fatalf("exempt request got rate limit headers")
		}
	}
}

// recordingLimiter records the keys it is asked about.
type recordingLimiter struct {
	IRateLimiter
	mu   sync.Mutex
	keys []string
}

func (l *recordingLimiter) Allow(ctx context.Context, key string, rate Rate) (RateLimitResult, error) {
	l.mu.Lock()
	l.keys = append(l.keys, key)
	l.mu.Unlock()
	return l.IRateLimiter.Allow(ctx, key, rate)
}

type limiterFunc func(context.Context, string, Rate) (RateLimitResult, error)

func (f limiterFunc) Allow(ctx context.Context, key string, rate Rate) (RateLimitResult, error) {
	return f(ctx, key, rate)
}

func TestRateLimitWith(t *testing.T) {
	shared := &recordingLimiter{IRateLimiter: NewMemoryRateLimiter(0)}
	provider := func() IRateLimiter { return shared }
	cfg := Config{Routes: []RouteConfig{
		{Name: "a", Method: "GET", Path: "/a", Handler: EmptyHandler(),
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(RateLimitWith(RateLimitConfig{Limit: 1, Prefix: "a:"}))}},
		{Name: "b", Method: "GET", Path: "/b", Handler: EmptyHandler(),
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(RateLimitWith(RateLimitConfig{Limit: 1, Prefix: "b:"}))}},
	}}
	s := newTestServer(t, cfg, provider)

	for i, tc := range []struct {
		path   string
		status int
	}{{"/a", 200}, {"/b", 200}, {"/a", 429}, {"/b", 429}} {
		if w, _ := do(t, s, get(tc.path, "203.0.113.1:1")); w.Code != tc.status {
			t.Fatalf("request %d: status %d, want %d", i, w.Code, tc.status)
		}
	}
	if want := []string{"a:203.0.113.1", "b:203.0.113.1", "a:203.0.113.1", "b:203.0.113.1"}; fmt.Sprint(shared.keys) != fmt.Sprint(want) {
		t.Fatalf("keys %v, want %v", shared.keys, want)
	}

	if err := newServerErr(t, cfg); !errors.Is(err, forgo.ErrMissingDependency) {
		t.Fatalf("without a limiter: err %v", err)
	}
}

func TestRateLimitLimiterFailure(t *testing.T) {
	failing := limiterFunc(func(context.Context, string, Rate) (RateLimitResult, error) {
		return RateLimitResult{}, errors.New("redis: connection refused")
	})
	provider := func() IRateLimiter { return failing }

	for _, tc := range []struct {
		failOpen bool
		status   int
		code     string
	}{
		{false, http.StatusServiceUnavailable, "rate_limit_unavailable"},
		{true, http.StatusOK, ""},
	} {
		limit := forgo.MiddlewareAs(RateLimitWith(RateLimitConfig{FailOpen: tc.failOpen}))
		s := newTestServer(t, Config{Routes: rateLimitedRoutes(limit)}, provider)
		w, envelope := do(t, s, get("/a", "203.0.113.1:1"))
		if w.Code != tc.status || errCode(envelope) != tc.code {
			t.Fatalf("failOpen=%v: %d %q", tc.failOpen, w.Code, errCode(envelope))
		}
	}
}

func TestRateLimitDisableHeaders(t *testing.T) {
	limit := forgo.MiddlewareAs(RateLimit(RateLimitConfig{Limit: 1, DisableHeaders: true}))
	s := newTestServer(t, Config{Routes: rateLimitedRoutes(limit)})
	for i, status := range []int{http.StatusOK, http.StatusTooManyRequests} {
		w, _ := do(t, s, get("/a", "203.0.113.1:1"))
		if w.Code != status || w.Header().Get(rateLimitLimitHeader) != "" || w.Header().Get(rateLimitRemainingHeader) != "" {
			t.Fatalf("request %d: %d %v", i, w.Code, w.Header())
		}
		if status == http.StatusTooManyRequests && w.Header().Get(retryAfterHeader) != "60" {
			t.Fatalf("rejection without Retry-After: %v", w.Header())
		}
	}
}

func TestRateLimitConfigErrors(t *testing.T) {
	for name, rl := range map[string]RateLimitConfig{
		"negative limit":  {Limit: -1},
		"negative period": {Period: -time.Second},
		"negative burst":  {Burst: -1},
	} {
		if err := newServerErr(t, Config{Routes: rateLimitedRoutes(forgo.MiddlewareAs(RateLimit(rl)))}); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := RateLimitWith(RateLimitConfig{})(nil); err == nil {
		t.Error("nil limiter: no error")
	}
}

// liveClaims returns claims valid now, with extra merged in.
func liveClaims(extra map[string]any) map[string]any {
	c := map[string]any{
		"sub": "user-1", "iss": "https://idp.example", "aud": "api",
		"iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(),
	}
	for k, v := range extra {
		c[k] = v
	}
	return c
}

func TestJWTAuth(t *testing.T) {
	k := jwtTestKeys()
	keys := JWTKeys{{ID: "k1", Algorithm: "HS256", Key: k.secret}}
	valid := signJWT(t, map[string]any{"alg": "HS256", "kid": "k1"}, liveClaims(nil), k.secret)
	expired := signJWT(t, map[string]any{"alg": "HS256", "kid": "k1"}, liveClaims(map[string]any{"exp": time.Now().Add(-time.Minute).Unix()}), k.secret)
	foreign := signJWT(t, map[string]any{"alg": "HS256", "kid": "k1"}, liveClaims(map[string]any{"iss": "https://evil.example"}), k.secret)

	withBaseMiddlewares(t, func(t *testing.T, base bool) {
		required := forgo.MiddlewareAs(JWTAuth(JWTConfig{Keys: keys, Issuer: "https://idp.example", Audience: []string{"api"}, Cookie: "session"}))
		optional := forgo.MiddlewareAs(JWTAuth(JWTConfig{Keys: keys, Optional: true}))
		cfg := Config{Routes: []RouteConfig{
			{Name: "me", Method: "GET", Path: "/me", Handler: HandlerAs[struct{}, whoAmIResp](newWhoAmIHandler), Middlewares: []forgo.MiddlewareFactory{required}},
			{Name: "maybe", Method: "GET", Path: "/maybe", Handler: HandlerAs[struct{}, whoAmIResp](newWhoAmIHandler), Middlewares: []forgo.MiddlewareFactory{optional}},
		}}
		cfg.DisableBaseMiddlewares = !base
		s := newTestServer(t, cfg)

		for _, tc := range []struct {
			name      string
			path      string
			auth      string
			cookie    string
			status    int
			code      string
			subject   string
			challenge string
		}{
			{name: "no token", path: "/me", status: 401, code: "unauthenticated", challenge: "Bearer"},
			{name: "other scheme", path: "/me", auth: "Basic dXNlcjpwYXNz", status: 401, code: "unauthenticated", challenge: "Bearer"},
			{name: "empty bearer", path: "/me", auth: "Bearer ", status: 401, code: "unauthenticated", challenge: "Bearer"},
			{name: "valid", path: "/me", auth: "Bearer " + valid, status: 200, subject: "user-1"},
			{name: "lowercase scheme", path: "/me", auth: "bearer " + valid, status: 200, subject: "user-1"},
			{name: "cookie", path: "/me", cookie: valid, status: 200, subject: "user-1"},
			{name: "expired", path: "/me", auth: "Bearer " + expired, status: 401, code: "token_expired", challenge: invalidBearerHeader},
			{name: "wrong issuer", path: "/me", auth: "Bearer " + foreign, status: 401, code: "invalid_token", challenge: invalidBearerHeader},
			{name: "garbage", path: "/me", auth: "Bearer garbage", status: 401, code: "invalid_token", challenge: invalidBearerHeader},
			{name: "optional without token", path: "/maybe", status: 200, subject: "anonymous"},
			{name: "optional with token", path: "/maybe", auth: "Bearer " + valid, status: 200, subject: "user-1"},
			{name: "optional with invalid token", path: "/maybe", auth: "Bearer " + expired, status: 401, code: "token_expired", challenge: invalidBearerHeader},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := httptest.NewRequest("GET", tc.path, nil)
				if tc.auth != "" {
					r.Header.Set(authorizationHeader, tc.auth)
				}
				if tc.cookie != "" {
					r.AddCookie(&http.Cookie{Name: "session", Value: tc.cookie})
				}
				w, envelope := do(t, s, r)
				if w.Code != tc.status || errCode(envelope) != tc.code {
					t.Fatalf("%d %q, want %d %q (%s)", w.Code, errCode(envelope), tc.status, tc.code, w.Body)
				}
				if got := w.Header().Get(wwwAuthenticateHeader); got != tc.challenge {
					t.Fatalf("%s = %q, want %q", wwwAuthenticateHeader, got, tc.challenge)
				}
				if tc.subject != "" {
					result, _ := envelope["result"].(map[string]any)
					if result["subject"] != tc.subject {
						t.Fatalf("subject %v, want %s", result["subject"], tc.subject)
					}
				}
			})
		}
	})
}

func TestJWTAuthKeySetUnavailable(t *testing.T) {
	srv := newJWKSServer(t)
	srv.serve(http.StatusBadGateway)
	auth := forgo.MiddlewareAs(JWTAuth(JWTConfig{Keys: &JWKS{URL: srv.URL}}))
	s := newTestServer(t, serverMiddlewares(Config{Routes: testRoutes()}, true, auth))

	r := httptest.NewRequest("GET", "/health", nil)
	r.Header.Set(authorizationHeader, "Bearer "+signJWT(t, map[string]any{"alg": "RS256", "kid": "r"}, liveClaims(nil), jwtTestKeys().rsa))
	w, envelope := do(t, s, r)
	if w.Code != http.StatusServiceUnavailable || errCode(envelope) != "auth_unavailable" || w.Header().Get(wwwAuthenticateHeader) != "" {
		t.Fatalf("%d %v %v", w.Code, envelope, w.Header())
	}
}

func TestJWTAuthConfigErrors(t *testing.T) {
	for name, jc := range map[string]JWTConfig{
		"no keys":          {},
		"empty key list":   {Keys: JWTKeys{}},
		"short secret":     {Keys: JWTKeys{{Key: []byte("short")}}},
		"bad JWKS URL":     {Keys: &JWKS{URL: "idp.example/jwks"}},
		"negative leeway":  {Keys: JWTKeys{{Key: jwtTestKeys().secret}}, Leeway: -time.Second},
		"unsupported type": {Keys: JWTKeys{{Key: "secret"}}},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := serverMiddlewares(Config{Routes: testRoutes()}, true, forgo.MiddlewareAs(JWTAuth(jc)))
			if err := newServerErr(t, cfg); err == nil || !strings.Contains(err.Error(), "http: jwt:") {
				t.Fatalf("err %v", err)
			}
		})
	}

	if _, ok := JWTClaimsFromCtx(context.Background()); ok {
		t.Fatal("claims in a context JWTAuth never saw")
	}
}

// TestMiddlewareStack checks the documented order: CORS answers preflights before the rate limit
// counts them or authentication rejects them, and rejections still carry the CORS headers.
func TestMiddlewareStack(t *testing.T) {
	k := jwtTestKeys()
	cfg := serverMiddlewares(Config{Routes: testRoutes()}, true,
		forgo.MiddlewareAs(CORS(CORSConfig{AllowOrigins: []string{"https://app.example"}})),
		forgo.MiddlewareAs(RateLimit(RateLimitConfig{Limit: 1})),
		forgo.MiddlewareAs(JWTAuth(JWTConfig{Keys: JWTKeys{{Key: k.secret}}})),
	)
	s := newTestServer(t, cfg)

	for range 3 {
		if w, _ := do(t, s, preflight("/health", "https://app.example")); w.Code != http.StatusNoContent {
			t.Fatalf("preflight: %d", w.Code)
		}
	}

	token := signJWT(t, map[string]any{"alg": "HS256"}, liveClaims(nil), k.secret)
	for i, want := range []int{http.StatusOK, http.StatusTooManyRequests} {
		r := withOrigin(httptest.NewRequest("GET", "/health", nil), "https://app.example")
		r.Header.Set(authorizationHeader, "Bearer "+token)
		w, _ := do(t, s, r)
		if w.Code != want || w.Header().Get(allowOriginHeader) != "https://app.example" {
			t.Fatalf("request %d: %d, %s %q", i, w.Code, allowOriginHeader, w.Header().Get(allowOriginHeader))
		}
	}
}

// TestConstructorsReportConfigErrors checks that a config processed once, when the constructor is
// created, still fails every middleware bootstrap asks the constructor for.
func TestConstructorsReportConfigErrors(t *testing.T) {
	for name, constructor := range map[string]func() (operation.Middleware, error){
		"CORS":      CORS(CORSConfig{AllowCredentials: true}),
		"RateLimit": RateLimit(RateLimitConfig{Limit: -1}),
		"JWTAuth":   JWTAuth(JWTConfig{}),
	} {
		for i := range 2 {
			if mw, err := constructor(); err == nil || mw != nil {
				t.Fatalf("%s call %d: middleware %v, err %v", name, i, mw != nil, err)
			}
		}
	}
	if _, err := RateLimitWith(RateLimitConfig{Limit: -1})(NewMemoryRateLimiter(0)); err == nil {
		t.Fatal("RateLimitWith: no error")
	}
}
