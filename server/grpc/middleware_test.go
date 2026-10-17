package grpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/jwttest"
	"github.com/efegreat/forgo/jwtauth"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/ratelimit"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// whoAmI answers with the subject JWTAuth verified, or "anonymous", as the username.
func whoAmI(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	// Claims survive contexts derived from the operation.
	derived, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if c, ok := jwtauth.ClaimsFromCtx(derived); ok {
		return &testpb.SimpleResponse{Username: c.Subject}, nil
	}
	return &testpb.SimpleResponse{Username: "anonymous"}, nil
}

func withToken(ctx context.Context, authorization string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
}

// newServerErr returns the error of New for cfg.
func newServerErr(t *testing.T, cfg Config, providers ...any) error {
	t.Helper()
	boot := forgo.NewBootstrap(nil, append([]any{newTestService}, providers...))
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	_, err = cfg.New(r)
	return err
}

func TestJWTAuth(t *testing.T) {
	k := jwttest.Keys()
	keys := jwtauth.Keys{{ID: "k1", Algorithm: "HS256", Key: k.Secret}}
	sign := func(extra map[string]any) string {
		return jwttest.Sign(t, map[string]any{"alg": "HS256", "kid": "k1"}, jwttest.Claims(extra), k.Secret)
	}
	valid := sign(nil)
	expired := sign(map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})
	foreign := sign(map[string]any{"iss": "https://evil.example"})

	for _, tc := range []struct {
		name          string
		cfg           JWTConfig
		authorization string
		code          codes.Code
		msg, reason   string
		subject       string
	}{
		{name: "valid", authorization: "Bearer " + valid, subject: "user-1"},
		{name: "lower-case scheme", authorization: "bearer " + valid, subject: "user-1"},
		{name: "no token", code: codes.Unauthenticated, msg: "authentication required", reason: "unauthenticated"},
		{name: "other scheme", authorization: "Basic dXNlcjpwYXNz", code: codes.Unauthenticated, msg: "authentication required", reason: "unauthenticated"},
		{name: "expired", authorization: "Bearer " + expired, code: codes.Unauthenticated, msg: "token expired", reason: "token_expired"},
		{name: "bad signature", authorization: "Bearer " + valid[:len(valid)-2], code: codes.Unauthenticated, msg: "invalid token", reason: "invalid_token"},
		{name: "wrong issuer", cfg: JWTConfig{Issuer: "https://idp.example"}, authorization: "Bearer " + foreign,
			code: codes.Unauthenticated, msg: "invalid token", reason: "invalid_token"},
		{name: "optional without token", cfg: JWTConfig{Optional: true}, subject: "anonymous"},
		{name: "optional with invalid token", cfg: JWTConfig{Optional: true}, authorization: "Bearer x.y.z",
			code: codes.Unauthenticated, msg: "invalid token", reason: "invalid_token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.cfg.Keys = keys
			h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(whoAmI, JWTAuth(tc.cfg))}})
			ctx := testCtx(t)
			if tc.authorization != "" {
				ctx = withToken(ctx, tc.authorization)
			}
			resp, err := h.client.UnaryCall(ctx, &testpb.SimpleRequest{})
			if tc.code != codes.OK {
				assertStatus(t, err, tc.code, tc.msg, tc.reason)
				if failed := h.line(t, "operation failed"); failed["err_code"] != tc.reason {
					t.Fatalf("logged %v", failed)
				}
				return
			}
			if err != nil || resp.GetUsername() != tc.subject {
				t.Fatalf("got %v, %v; want %s", resp, err, tc.subject)
			}
		})
	}
}

func TestJWTAuthKeySetUnavailable(t *testing.T) {
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(idp.Close)
	auth := JWTAuth(JWTConfig{Keys: &jwtauth.JWKS{URL: idp.URL}})
	h := newHarness(t, Config{Services: []ServiceConfig{unaryConfig(whoAmI, auth)}})

	token := jwttest.Sign(t, map[string]any{"alg": "RS256", "kid": "r"}, jwttest.Claims(nil), jwttest.Keys().RSA)
	_, err := h.client.UnaryCall(withToken(testCtx(t), "Bearer "+token), &testpb.SimpleRequest{})
	assertStatus(t, err, codes.Unavailable, "authentication unavailable", "auth_unavailable")
}

// A stream is authenticated once, before its handler runs.
func TestJWTAuthStreams(t *testing.T) {
	svc := testServiceConfig()
	svc.Middlewares = []forgo.MiddlewareFactory{JWTAuth(JWTConfig{Keys: jwtauth.Keys{{Key: jwttest.Keys().Secret}}})}
	h := newHarness(t, Config{Services: []ServiceConfig{svc}})
	req := &testpb.StreamingOutputCallRequest{ResponseParameters: []*testpb.ResponseParameters{{Size: 1}, {Size: 2}}}

	stream, err := h.client.StreamingOutputCall(testCtx(t), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	assertStatus(t, err, codes.Unauthenticated, "authentication required", "unauthenticated")

	token := jwttest.Sign(t, map[string]any{"alg": "HS256"}, jwttest.Claims(nil), jwttest.Keys().Secret)
	stream, err = h.client.StreamingOutputCall(withToken(testCtx(t), "Bearer "+token), req)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestJWTAuthConfigErrors(t *testing.T) {
	for name, c := range map[string]JWTConfig{
		"no keys":         {},
		"short secret":    {Keys: jwtauth.Keys{{Key: []byte("short")}}},
		"bad JWKS URL":    {Keys: &jwtauth.JWKS{URL: "idp.example/jwks"}},
		"negative leeway": {Keys: jwtauth.Keys{{Key: jwttest.Keys().Secret}}, Leeway: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := Config{Services: []ServiceConfig{unaryConfig(whoAmI, JWTAuth(c))}}
			if err := newServerErr(t, cfg); err == nil || !strings.Contains(err.Error(), "grpc: jwtauth:") {
				t.Fatalf("err %v", err)
			}
		})
	}
}

// rateLimited serves UnaryCall behind limit.
func rateLimited(limit forgo.MiddlewareFactory) Config {
	return Config{Services: []ServiceConfig{unaryConfig(whoAmI, limit)}}
}

// callHeader makes a UnaryCall and returns its header metadata.
func callHeader(t *testing.T, client testpb.TestServiceClient, ctx context.Context) (metadata.MD, error) {
	t.Helper()
	var header metadata.MD
	_, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Header(&header))
	return header, err
}

func TestRateLimit(t *testing.T) {
	h := newHarness(t, rateLimited(RateLimit(RateLimitConfig{Limit: 2, Period: time.Minute})))
	for i, want := range []struct {
		remaining, reset string
	}{{"1", "30"}, {"0", "60"}} {
		header, err := callHeader(t, h.client, testCtx(t))
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if header.Get(rateLimitLimitMetadata)[0] != "2" || header.Get(rateLimitRemainingMetadata)[0] != want.remaining ||
			header.Get(rateLimitResetMetadata)[0] != want.reset || len(header.Get(retryAfterMetadata)) != 0 {
			t.Fatalf("call %d: header %v", i, header)
		}
	}

	header, err := callHeader(t, h.client, testCtx(t))
	assertStatus(t, err, codes.ResourceExhausted, "too many requests", "rate_limited")
	if header.Get(retryAfterMetadata)[0] != "30" || header.Get(rateLimitRemainingMetadata)[0] != "0" {
		t.Fatalf("header %v", header)
	}
	if failed := h.line(t, "operation failed"); failed["err_code"] != "rate_limited" || failed["grpc_code"] != "ResourceExhausted" {
		t.Fatalf("logged %v", failed)
	}
}

func TestRateLimitDisableMetadata(t *testing.T) {
	h := newHarness(t, rateLimited(RateLimit(RateLimitConfig{Limit: 1, DisableMetadata: true})))
	header, err := callHeader(t, h.client, testCtx(t))
	if err != nil || len(header.Get(rateLimitLimitMetadata)) != 0 {
		t.Fatalf("header %v, %v", header, err)
	}
	header, err = callHeader(t, h.client, testCtx(t))
	if status.Code(err) != codes.ResourceExhausted || header.Get(retryAfterMetadata)[0] != "60" ||
		len(header.Get(rateLimitRemainingMetadata)) != 0 {
		t.Fatalf("header %v, %v", header, err)
	}
}

func TestRateLimitKeys(t *testing.T) {
	tenant := func(ctx *operation.Context) (string, error) {
		values := ctx.State.(*State).Metadata().Get("x-tenant")
		if len(values) == 0 {
			return "", nil // exempt
		}
		return values[0], nil
	}
	h := newHarness(t, rateLimited(RateLimit(RateLimitConfig{Limit: 1, Key: tenant})))
	as := func(name string) context.Context {
		return metadata.AppendToOutgoingContext(testCtx(t), "x-tenant", name)
	}

	for _, name := range []string{"a", "b"} {
		if _, err := h.client.UnaryCall(as(name), &testpb.SimpleRequest{}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := h.client.UnaryCall(as("a"), &testpb.SimpleRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a again: %v", err)
	}
	for range 3 {
		header, err := callHeader(t, h.client, testCtx(t))
		if err != nil || len(header.Get(rateLimitLimitMetadata)) != 0 {
			t.Fatalf("exempt call: %v, header %v", err, header)
		}
	}
}

// limiterFunc is a ratelimit.ILimiter of a function.
type limiterFunc func(context.Context, string, ratelimit.Rate) (ratelimit.Result, error)

func (f limiterFunc) Allow(ctx context.Context, key string, rate ratelimit.Rate) (ratelimit.Result, error) {
	return f(ctx, key, rate)
}

func TestRateLimitWith(t *testing.T) {
	var keys []string
	shared := ratelimit.NewMemory(0)
	recording := limiterFunc(func(ctx context.Context, key string, rate ratelimit.Rate) (ratelimit.Result, error) {
		keys = append(keys, key)
		return shared.Allow(ctx, key, rate)
	})
	provider := func() ratelimit.ILimiter { return recording }
	limit := RateLimitWith(RateLimitConfig{Limit: 1, Prefix: "grpc:"})
	h := newHarness(t, rateLimited(limit), provider)

	if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second call: %v", err)
	}
	if len(keys) != 2 || keys[0] != "grpc:bufconn" {
		t.Fatalf("keys %v", keys)
	}
}

func TestRateLimitLimiterFailure(t *testing.T) {
	failing := limiterFunc(func(context.Context, string, ratelimit.Rate) (ratelimit.Result, error) {
		return ratelimit.Result{}, errors.New("redis: connection refused")
	})
	provider := func() ratelimit.ILimiter { return failing }

	closed := newHarness(t, rateLimited(RateLimitWith(RateLimitConfig{})), provider)
	_, err := closed.client.UnaryCall(testCtx(t), &testpb.SimpleRequest{})
	assertStatus(t, err, codes.Unavailable, "rate limiting unavailable", "rate_limit_unavailable")

	open := newHarness(t, rateLimited(RateLimitWith(RateLimitConfig{FailOpen: true})), provider)
	header, err := callHeader(t, open.client, testCtx(t))
	if err != nil || len(header.Get(rateLimitLimitMetadata)) != 0 {
		t.Fatalf("fail open: %v, header %v", err, header)
	}
	if line := open.line(t, "rate limiter failed, request admitted"); line["error"] != "redis: connection refused" {
		t.Fatalf("logged %v", line)
	}
}

// A stream counts once, when it starts, however many messages it carries.
func TestRateLimitStreams(t *testing.T) {
	svc := testServiceConfig()
	svc.Middlewares = []forgo.MiddlewareFactory{RateLimit(RateLimitConfig{Limit: 1})}
	h := newHarness(t, Config{Services: []ServiceConfig{svc}})
	req := &testpb.StreamingOutputCallRequest{ResponseParameters: []*testpb.ResponseParameters{{Size: 1}, {Size: 1}, {Size: 1}}}

	stream, err := h.client.StreamingOutputCall(testCtx(t), req)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	stream, err = h.client.StreamingOutputCall(testCtx(t), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	assertStatus(t, err, codes.ResourceExhausted, "too many requests", "rate_limited")
}

// Behind trusted proxies, calls are counted by the client the x-forwarded-for metadata names; otherwise
// the metadata is ignored.
func TestRateLimitTrustedProxies(t *testing.T) {
	loopback := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	from := func(addr string) context.Context {
		return metadata.AppendToOutgoingContext(testCtx(t), "x-forwarded-for", addr)
	}

	behind := tcpClient(t, rateLimited(RateLimit(RateLimitConfig{Limit: 1, TrustedProxies: loopback})))
	for _, addr := range []string{"198.51.100.1", "198.51.100.2", "2001:db8::1"} {
		if _, err := behind.UnaryCall(from(addr), &testpb.SimpleRequest{}); err != nil {
			t.Fatalf("%s: %v", addr, err)
		}
	}
	for _, addr := range []string{"198.51.100.1", "2001:db8::2"} { // the same /64
		if _, err := behind.UnaryCall(from(addr), &testpb.SimpleRequest{}); status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("%s again: %v", addr, err)
		}
	}

	direct := tcpClient(t, rateLimited(RateLimit(RateLimitConfig{Limit: 1})))
	if _, err := direct.UnaryCall(from("198.51.100.1"), &testpb.SimpleRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := direct.UnaryCall(from("198.51.100.2"), &testpb.SimpleRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a spoofed address got its own limit: %v", err)
	}
}

// tcpClient serves cfg on a TCP port of the loopback interface, so that calls have a peer IP.
func tcpClient(t *testing.T, cfg Config) testpb.TestServiceClient {
	t.Helper()
	s := newServer(t, cfg, &syncBuffer{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = s.g.Serve(lis)
	}()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		s.g.Stop()
		<-served
	})
	return testpb.NewTestServiceClient(conn)
}

func TestRateLimitClientKey(t *testing.T) {
	st := func(addr net.Addr) *State {
		ctx := context.Background()
		if addr != nil {
			ctx = peer.NewContext(ctx, &peer.Peer{Addr: addr})
		}
		return &State{stream: fakeStream{ctx: ctx}}
	}
	for _, tc := range []struct {
		addr net.Addr
		want string
	}{
		{&net.TCPAddr{IP: net.ParseIP("203.0.113.7"), Port: 1}, "203.0.113.7"}, // IPv4-mapped in net.IP
		{&net.TCPAddr{IP: net.ParseIP("2001:db8:1:2:3:4:5:6"), Port: 1}, "2001:db8:1:2::/64"},
		{&net.UnixAddr{Name: "/run/app.sock", Net: "unix"}, "/run/app.sock"},
		{nil, unknownPeerKey},
	} {
		if got := clientKey(st(tc.addr), nil); got != tc.want {
			t.Errorf("clientKey(%v) = %q, want %q", tc.addr, got, tc.want)
		}
	}
}

func TestRateLimitConfigErrors(t *testing.T) {
	cfg := rateLimited(RateLimit(RateLimitConfig{Limit: -1}))
	if err := newServerErr(t, cfg); err == nil || !strings.Contains(err.Error(), "grpc: rate limit:") {
		t.Fatalf("err %v", err)
	}
	if err := newServerErr(t, rateLimited(RateLimitWith(RateLimitConfig{}))); !errors.Is(err, forgo.ErrMissingDependency) {
		t.Fatalf("without a limiter: err %v", err)
	}
	if c := (RateLimitConfig{Limit: 5}).Sanitize(); c.Period != time.Minute || c.Burst != 5 {
		t.Fatalf("sanitized %+v", c)
	}
}
