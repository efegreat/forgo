package http

import (
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/jwtauth"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/ratelimit"
)

const (
	originHeader             = "Origin"
	varyHeader               = "Vary"
	authorizationHeader      = "Authorization"
	wwwAuthenticateHeader    = "WWW-Authenticate"
	retryAfterHeader         = "Retry-After"
	rateLimitLimitHeader     = "Ratelimit-Limit" // canonical, so setting it on every request does not allocate
	rateLimitRemainingHeader = "Ratelimit-Remaining"
	rateLimitResetHeader     = "Ratelimit-Reset"
	allowOriginHeader        = "Access-Control-Allow-Origin"
	allowCredentialsHeader   = "Access-Control-Allow-Credentials"
	allowMethodsHeader       = "Access-Control-Allow-Methods"
	allowHeadersHeader       = "Access-Control-Allow-Headers"
	exposeHeadersHeader      = "Access-Control-Expose-Headers"
	maxAgeHeader             = "Access-Control-Max-Age"
	requestMethodHeader      = "Access-Control-Request-Method"
	requestHeadersHeader     = "Access-Control-Request-Headers"
	preflightVary            = "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"

	defaultCORSMaxAge   = 10 * time.Minute
	bearerChallenge     = "Bearer"
	invalidBearerHeader = `Bearer error="invalid_token"`
)

// ErrorResponder answers the error of an operation that failed without writing a response, such as a middleware
// that rejected the request by returning an error instead of calling next, or the *middleware.PanicError of a
// Recovery inside it. The error is mapped by em and written through sf. A response already written is kept.
//
// The operation fails with the error itself when it holds an operation.Error, so the log keeps what is wrapped
// around it, and with the mapped error otherwise.
func ErrorResponder(sf SerializerFactory, em *operation.ErrorMapper) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			return respondError(ctx, sf, em, next(ctx))
		}
	}
}

// CORSConfig configures CORS. The zero value allows every origin without credentials.
type CORSConfig struct {
	// AllowOrigins are the origins allowed to make cross-origin requests, such as "https://app.example.com".
	// "*" allows every origin. One "*" inside an origin matches any non-empty text, so "https://*.example.com"
	// allows every subdomain and "http://localhost:*" every port. Default: "*", unless AllowOriginFunc is set.
	AllowOrigins []string
	// AllowOriginFunc decides the origins AllowOrigins does not allow, for origins only known at runtime.
	AllowOriginFunc func(origin string) bool
	// AllowMethods are the methods allowed in cross-origin requests.
	// Default: GET, HEAD, PUT, PATCH, POST, DELETE.
	AllowMethods []string
	// AllowHeaders are the request headers allowed in cross-origin requests.
	// Default: whatever headers a preflight request asks for.
	AllowHeaders []string
	// ExposeHeaders are the response headers scripts may read.
	// Default: X-Request-ID, Retry-After and the RateLimit headers.
	ExposeHeaders []string
	// AllowCredentials lets browsers send cookies and HTTP authentication with cross-origin requests.
	// It cannot be combined with the "*" origin, which would let every site act on behalf of your users.
	AllowCredentials bool
	// MaxAge is how long browsers may cache a preflight response. Default: 10 minutes. Negative disables caching.
	MaxAge time.Duration
}

// Sanitize returns a copy of c with defaults applied.
func (c CORSConfig) Sanitize() CORSConfig {
	if len(c.AllowOrigins) == 0 && c.AllowOriginFunc == nil {
		c.AllowOrigins = []string{"*"}
	}
	if len(c.AllowMethods) == 0 {
		c.AllowMethods = []string{
			http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete,
		}
	}
	if len(c.ExposeHeaders) == 0 {
		c.ExposeHeaders = []string{
			ReqIDHeader, retryAfterHeader, rateLimitLimitHeader, rateLimitRemainingHeader, rateLimitResetHeader,
		}
	}
	if c.MaxAge == 0 {
		c.MaxAge = defaultCORSMaxAge
	}
	return c
}

// CORS returns the factory of a middleware implementing cross-origin resource sharing. It answers
// preflight requests itself with 204 and adds the CORS headers to every other response, error
// responses included.
//
// Preflight requests use the OPTIONS method, so they match no route of another method. Attach CORS
// to the server middlewares, which also run for unmatched requests, rather than to routes.
//
// The config is sanitized and compiled once, here. Every middleware the factory creates shares the
// result, and an invalid config fails the server's New.
func CORS(cfg CORSConfig) forgo.MiddlewareFactory {
	p, err := newCORSPolicy(cfg.Sanitize())
	return func(forgo.Resolver) (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		return p.middleware, nil
	}
}

type corsPolicy struct {
	anyOrigin   bool
	origins     map[string]struct{}
	patterns    []originPattern
	originFunc  func(string) bool
	credentials bool
	methods     string
	headers     string // empty: the requested headers are allowed
	expose      string
	maxAge      string
}

type originPattern struct{ prefix, suffix string }

func newCORSPolicy(c CORSConfig) (*corsPolicy, error) {
	p := &corsPolicy{
		origins:     make(map[string]struct{}, len(c.AllowOrigins)),
		originFunc:  c.AllowOriginFunc,
		credentials: c.AllowCredentials,
		headers:     strings.Join(c.AllowHeaders, ", "),
		expose:      strings.Join(c.ExposeHeaders, ", "),
		maxAge:      strconv.Itoa(int(max(c.MaxAge, 0) / time.Second)),
	}

	for _, o := range c.AllowOrigins {
		o = strings.ToLower(strings.TrimSpace(o))
		if o == "*" {
			p.anyOrigin = true
			continue
		}
		if err := validateOrigin(o); err != nil {
			return nil, fmt.Errorf("http: cors: %w", err)
		}
		if prefix, suffix, ok := strings.Cut(o, "*"); ok {
			p.patterns = append(p.patterns, originPattern{prefix: prefix, suffix: suffix})
		} else {
			p.origins[o] = struct{}{}
		}
	}
	if p.anyOrigin && p.credentials {
		return nil, errors.New(`http: cors: AllowCredentials cannot be combined with the "*" origin`)
	}

	methods := make([]string, len(c.AllowMethods))
	for i, m := range c.AllowMethods {
		methods[i] = strings.ToUpper(strings.TrimSpace(m))
	}
	p.methods = strings.Join(methods, ", ")

	return p, nil
}

// validateOrigin accepts scheme://host[:port], with at most one "*" after the scheme.
func validateOrigin(o string) error {
	scheme, host, ok := strings.Cut(o, "://")
	switch {
	case !ok || scheme == "" || host == "" || strings.ContainsAny(host, "/?#"):
		return fmt.Errorf("invalid origin %q: want scheme://host[:port]", o)
	case strings.Contains(scheme, "*") || strings.Count(host, "*") > 1:
		return fmt.Errorf("invalid origin %q: only one \"*\" after the scheme is allowed", o)
	}
	return nil
}

// allowOrigin returns the Access-Control-Allow-Origin value for origin, or "" when it is not allowed.
func (p *corsPolicy) allowOrigin(origin string) string {
	if p.anyOrigin {
		return "*"
	}
	o := strings.ToLower(origin)
	if _, ok := p.origins[o]; ok {
		return origin
	}
	for _, pt := range p.patterns {
		if len(o) > len(pt.prefix)+len(pt.suffix) && strings.HasPrefix(o, pt.prefix) && strings.HasSuffix(o, pt.suffix) {
			return origin
		}
	}
	if p.originFunc != nil && p.originFunc(origin) {
		return origin
	}
	return ""
}

func (p *corsPolicy) middleware(next operation.Handler) operation.Handler {
	return func(ctx *operation.Context) error {
		st := ctx.State.(*State)
		r := st.Request
		h := st.Writer.Header()
		origin := r.Header.Get(originHeader)

		if origin != "" && r.Method == http.MethodOptions && r.Header.Get(requestMethodHeader) != "" {
			h.Add(varyHeader, preflightVary)
			if allowed := p.allowOrigin(origin); allowed != "" {
				h.Set(allowOriginHeader, allowed)
				if p.credentials {
					h.Set(allowCredentialsHeader, "true")
				}
				h.Set(allowMethodsHeader, p.methods)
				if p.headers != "" {
					h.Set(allowHeadersHeader, p.headers)
				} else if requested := r.Header.Get(requestHeadersHeader); requested != "" {
					h.Set(allowHeadersHeader, requested)
				}
				h.Set(maxAgeHeader, p.maxAge)
			}
			// A disallowed origin gets no CORS headers, which makes the browser fail the request.
			st.Writer.WriteHeader(http.StatusNoContent)
			return nil
		}

		// Unless every origin gets "*", responses depend on Origin, also those to requests without one.
		if !p.anyOrigin {
			h.Add(varyHeader, originHeader)
		}
		if origin != "" {
			if allowed := p.allowOrigin(origin); allowed != "" {
				h.Set(allowOriginHeader, allowed)
				if p.credentials {
					h.Set(allowCredentialsHeader, "true")
				}
				if p.expose != "" {
					h.Set(exposeHeadersHeader, p.expose)
				}
			}
		}
		return next(ctx)
	}
}

// RateLimitConfig configures RateLimit. The zero value admits 60 requests per minute per client IP.
type RateLimitConfig struct {
	// Limit is the number of requests admitted per Period. Default: 60.
	Limit int
	// Period is the time Limit applies to. Default: 1 minute.
	Period time.Duration
	// Burst is how many requests may arrive at once. Requests are refilled continuously,
	// one every Period/Limit. Default: Limit.
	Burst int
	// Key returns the key requests are counted under. An empty key exempts the request.
	// Default: the client IP, with IPv6 clients counted by their /64 network.
	Key func(*operation.Context) (string, error)
	// TrustedProxies are the networks of the reverse proxies in front of the server. The default Key
	// reads the client IP from X-Forwarded-For only behind them (see ClientIP). Without them the
	// header is ignored, because any client can send it.
	TrustedProxies []netip.Prefix
	// Prefix is prepended to every key. Give each RateLimitWith middleware sharing a limiter its own
	// Prefix, or they count into the same buckets.
	Prefix string
	// FailOpen admits requests when the limiter fails, logging the failure. By default those requests
	// fail with ratelimit.ErrUnavailable.
	FailOpen bool
	// DisableHeaders stops the RateLimit-Limit, RateLimit-Remaining and RateLimit-Reset response headers.
	// Rejected requests always get Retry-After.
	DisableHeaders bool
}

// Sanitize returns a copy of c with defaults applied.
func (c RateLimitConfig) Sanitize() RateLimitConfig {
	r := ratelimit.Config{Limit: c.Limit, Period: c.Period, Burst: c.Burst}.Sanitize()
	c.Limit, c.Period, c.Burst = r.Limit, r.Period, r.Burst
	return c
}

// RateLimit returns the factory of a middleware that rejects requests over the configured rate with
// ratelimit.ErrLimited (429) and a Retry-After header.
//
// Each middleware it creates counts in its own ratelimit.Memory: attached to the server, the rate
// applies to all routes together; attached to a route, to that route alone. The counts are per
// process. To share them between instances, use RateLimitWith.
//
// The config is validated once, here, and an invalid config fails the server's New. Its contract
// documents the 429 response and the headers.
func RateLimit(cfg RateLimitConfig) forgo.MiddlewareFactory {
	rl, err := newRateLimit(cfg)
	return withContract(rateLimitContract(cfg), func(forgo.Resolver) (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		return rl.middleware(ratelimit.NewMemory(0)), nil
	})
}

// RateLimitWith is RateLimit counting in the ratelimit.ILimiter of the bootstrap, such as one backed by
// a store shared between instances. The server's New fails when the bootstrap provides none.
func RateLimitWith(cfg RateLimitConfig) forgo.MiddlewareFactory {
	rl, err := newRateLimit(cfg)
	return withContract(rateLimitContract(cfg), func(r forgo.Resolver) (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		l, err := r.Resolve[ratelimit.ILimiter]()
		if err != nil {
			return nil, fmt.Errorf("http: rate limit: %w", err)
		}
		return rl.middleware(l), nil
	})
}

// rateLimitContract documents the 429 response of a RateLimitConfig and its headers.
func rateLimitContract(c RateLimitConfig) MiddlewareContract {
	seconds := reflect.TypeFor[uint]()
	contract := MiddlewareContract{Rejections: []Rejection{{
		Err: ratelimit.ErrLimited,
		Headers: []ResponseHeader{
			{Name: retryAfterHeader, Description: "Seconds until a request is admitted again.", Type: seconds},
		},
	}}}
	if !c.DisableHeaders {
		// Spelled as the IETF draft spells them, rather than as the canonical constants.
		contract.Headers = []ResponseHeader{
			{Name: "RateLimit-Limit", Description: "Requests admitted at once.", Type: seconds},
			{Name: "RateLimit-Remaining", Description: "Requests admitted now.", Type: seconds},
			{Name: "RateLimit-Reset", Description: "Seconds until RateLimit-Limit requests are admitted again.", Type: seconds},
		}
	}
	return contract
}

// rateLimit is the rule of a RateLimitConfig, with what responses need computed up front.
type rateLimit struct {
	rule           *ratelimit.Rule
	limit          string // RateLimit-Limit value
	disableHeaders bool
}

func newRateLimit(c RateLimitConfig) (*rateLimit, error) {
	key := c.Key
	if key == nil {
		proxies := slices.Clone(c.TrustedProxies)
		key = func(ctx *operation.Context) (string, error) {
			return clientKey(ctx.State.(*State).Request, proxies), nil
		}
	}
	rule, err := ratelimit.NewRule(ratelimit.Config{
		Limit: c.Limit, Period: c.Period, Burst: c.Burst, Key: key, Prefix: c.Prefix, FailOpen: c.FailOpen,
	})
	if err != nil {
		return nil, fmt.Errorf("http: rate limit: %w", err)
	}
	return &rateLimit{
		rule:           rule,
		limit:          strconv.Itoa(rule.Rate().Burst),
		disableHeaders: c.DisableHeaders,
	}, nil
}

// middleware returns the middleware counting in l.
func (rl *rateLimit) middleware(l ratelimit.ILimiter) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			res, counted, err := rl.rule.Admit(ctx, l)
			if err != nil {
				return err
			}
			if counted {
				h := ctx.State.(*State).Writer.Header()
				if !rl.disableHeaders {
					h.Set(rateLimitLimitHeader, rl.limit)
					h.Set(rateLimitRemainingHeader, strconv.Itoa(res.Remaining))
					h.Set(rateLimitResetHeader, ceilSeconds(res.ResetAfter))
				}
				if !res.Allowed {
					h.Set(retryAfterHeader, ceilSeconds(max(res.RetryAfter, time.Second)))
					return ratelimit.ErrLimited
				}
			}
			return next(ctx)
		}
	}
}

// ceilSeconds formats d as whole seconds, rounded up.
func ceilSeconds(d time.Duration) string {
	return strconv.FormatInt(int64((d+time.Second-1)/time.Second), 10)
}

// JWTConfig configures JWTAuth.
type JWTConfig struct {
	// Keys verify token signatures. Required: jwtauth.Keys for keys known up front, a *jwtauth.JWKS for
	// an identity provider's key set URL, or any jwtauth.IKeySet.
	Keys jwtauth.IKeySet
	// Issuer, when set, must equal the iss claim.
	Issuer string
	// Audience, when set, must contain one of the aud claim values.
	Audience []string
	// Leeway tolerates clock skew between the issuer and the server when checking exp and nbf.
	Leeway time.Duration
	// Cookie is the name of a cookie the token is read from when the request has no bearer token.
	Cookie string
	// Optional lets requests without a token through, without claims. A token that is sent must still be valid.
	Optional bool
}

// JWTAuth returns the factory of a middleware that authenticates requests with a JSON Web Token sent
// as "Authorization: Bearer <token>", or in Cookie. A jwtauth.Authenticator verifies it (see
// jwtauth.New for the rules).
//
// A request without a token fails with jwtauth.ErrUnauthenticated, one with an invalid token with
// jwtauth.ErrInvalidToken or jwtauth.ErrTokenExpired, all answered 401 with a WWW-Authenticate header.
// The verified claims are available downstream through jwtauth.ClaimsFromCtx.
//
// The config is validated and the verifier built once, here. Every route the factory is attached to
// shares them, and an invalid config fails the server's New. Its contract documents the token and
// the 401 response.
func JWTAuth(cfg JWTConfig) forgo.MiddlewareFactory {
	mw, err := newJWTAuth(cfg)
	return withContract(jwtContract(cfg), func(forgo.Resolver) (operation.Middleware, error) {
		return mw, err
	})
}

// jwtContract documents the token a JWTConfig reads and its 401 responses.
func jwtContract(c JWTConfig) MiddlewareContract {
	challenge := []ResponseHeader{{
		Name:        wwwAuthenticateHeader,
		Description: `Bearer, with error="invalid_token" for an invalid or expired token.`,
		Type:        reflect.TypeFor[string](),
	}}
	rejections := []Rejection{
		{Err: jwtauth.ErrInvalidToken, Headers: challenge},
		{Err: jwtauth.ErrTokenExpired, Headers: challenge},
	}
	if !c.Optional {
		rejections = append([]Rejection{{Err: jwtauth.ErrUnauthenticated, Headers: challenge}}, rejections...)
	}
	return MiddlewareContract{
		Auth:       &AuthContract{BearerFormat: "JWT", Cookie: c.Cookie, Optional: c.Optional},
		Rejections: rejections,
	}
}

func newJWTAuth(c JWTConfig) (operation.Middleware, error) {
	a, err := jwtauth.New(jwtauth.Config{
		Keys: c.Keys, Issuer: c.Issuer, Audience: c.Audience, Leeway: c.Leeway, Optional: c.Optional,
	})
	if err != nil {
		return nil, fmt.Errorf("http: %w", err)
	}
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			if err := a.Authenticate(ctx, bearerToken(st.Request, c.Cookie)); err != nil {
				switch {
				case errors.Is(err, jwtauth.ErrUnauthenticated):
					st.Writer.Header().Set(wwwAuthenticateHeader, bearerChallenge)
				case errors.Is(err, jwtauth.ErrInvalidToken), errors.Is(err, jwtauth.ErrTokenExpired):
					st.Writer.Header().Set(wwwAuthenticateHeader, invalidBearerHeader)
				}
				return err
			}
			return next(ctx)
		}
	}, nil
}

// bearerToken returns the bearer token of the Authorization header, or else the value of the cookie.
func bearerToken(r *http.Request, cookie string) string {
	if token := jwtauth.BearerToken(r.Header.Get(authorizationHeader)); token != "" {
		return token
	}
	if cookie != "" {
		if c, err := r.Cookie(cookie); err == nil {
			return c.Value
		}
	}
	return ""
}
