package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"

	"github.com/golang-jwt/jwt/v5"
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
	defaultRateLimit    = 60
	defaultRatePeriod   = time.Minute
	bearerChallenge     = "Bearer"
	invalidBearerHeader = `Bearer error="invalid_token"`
)

var (
	// ErrUnauthenticated rejects requests JWTAuth finds no token in.
	ErrUnauthenticated = operation.Error{
		Code:     "unauthenticated",
		Status:   operation.StatusUnauthenticated,
		Message:  "authentication required",
		Severity: operation.SeverityLight,
	}
	// ErrInvalidToken rejects requests whose token JWTAuth cannot verify or whose claims it does not accept.
	ErrInvalidToken = operation.Error{
		Code:     "invalid_token",
		Status:   operation.StatusUnauthenticated,
		Message:  "invalid token",
		Severity: operation.SeverityLight,
	}
	// ErrTokenExpired rejects requests whose token is otherwise valid but expired, so clients know to refresh it.
	ErrTokenExpired = operation.Error{
		Code:     "token_expired",
		Status:   operation.StatusUnauthenticated,
		Message:  "token expired",
		Severity: operation.SeverityLight,
	}
	// ErrAuthUnavailable fails requests when JWTAuth cannot get the keys to verify their token.
	ErrAuthUnavailable = operation.Error{
		Code:     "auth_unavailable",
		Status:   operation.StatusUnavailable,
		Message:  "authentication unavailable",
		Severity: operation.SeverityCritical,
	}
	// ErrRateLimited rejects requests over the rate of a RateLimit middleware.
	ErrRateLimited = operation.Error{
		Code:     "rate_limited",
		Status:   operation.StatusRateLimited,
		Message:  "too many requests",
		Severity: operation.SeverityLight,
	}
	// ErrRateLimitUnavailable fails requests when the rate limiter fails and FailOpen is off.
	ErrRateLimitUnavailable = operation.Error{
		Code:     "rate_limit_unavailable",
		Status:   operation.StatusUnavailable,
		Message:  "rate limiting unavailable",
		Severity: operation.SeverityCritical,
	}
)

// ErrorResponder writes the error response of an operation that failed without writing one,
// such as a middleware that rejected the request by returning an error instead of calling next.
func ErrorResponder(sf SerializerFactory, em *operation.ErrorMapper) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			return respondError(ctx, sf, em, next(ctx))
		}
	}
}

// respondError serializes err unless the response was already written or can no longer be written.
// It returns the error to report for the operation.
func respondError(ctx *operation.Context, sf SerializerFactory, em *operation.ErrorMapper, err error) error {
	if err == nil || errors.Is(err, http.ErrAbortHandler) || errors.Is(err, errBrokenConnection) {
		return err
	}

	st := ctx.State.(*State)
	if st.Writer.Written() {
		return err
	}

	return sf(ctx.ID, st.Writer).Error(em.Map(err))
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

// CORS returns a constructor, for forgo.MiddlewareAs, of a middleware implementing cross-origin
// resource sharing. It answers preflight requests itself with 204 and adds the CORS headers to
// every other response, error responses included.
//
// Preflight requests use the OPTIONS method, so they match no route of another method. Attach CORS
// to the server middlewares, which also run for unmatched requests, rather than to routes.
//
// The config is sanitized and compiled once, here. Every middleware the constructor returns shares
// the result, and an invalid config is reported when bootstrap calls the constructor.
func CORS(cfg CORSConfig) func() (operation.Middleware, error) {
	p, err := newCORSPolicy(cfg.Sanitize())
	return func() (operation.Middleware, error) {
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
	// fail with ErrRateLimitUnavailable.
	FailOpen bool
	// DisableHeaders stops the RateLimit-Limit, RateLimit-Remaining and RateLimit-Reset response headers.
	// Rejected requests always get Retry-After.
	DisableHeaders bool
}

func (c RateLimitConfig) Sanitize() RateLimitConfig {
	if c.Limit == 0 {
		c.Limit = defaultRateLimit
	}
	if c.Period == 0 {
		c.Period = defaultRatePeriod
	}
	if c.Burst == 0 {
		c.Burst = c.Limit
	}
	return c
}

// RateLimit returns a constructor, for forgo.MiddlewareAs, of a middleware that rejects requests
// over the configured rate with ErrRateLimited (429) and a Retry-After header.
//
// Each middleware it constructs counts in its own MemoryRateLimiter: attached to the server, the
// rate applies to all routes together; attached to a route, to that route alone. The counts are per
// process. To share them between instances, use RateLimitWith.
func RateLimit(cfg RateLimitConfig) func() (operation.Middleware, error) {
	rl, err := newRateLimit(cfg)
	return func() (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		return rl.middleware(NewMemoryRateLimiter(0)), nil
	}
}

// RateLimitWith is RateLimit counting in an IRateLimiter resolved by bootstrap, such as one backed
// by a store shared between instances.
func RateLimitWith(cfg RateLimitConfig) func(IRateLimiter) (operation.Middleware, error) {
	rl, err := newRateLimit(cfg)
	return func(l IRateLimiter) (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		if l == nil {
			return nil, errors.New("http: rate limit: nil limiter")
		}
		return rl.middleware(l), nil
	}
}

// rateLimit is a sanitized RateLimitConfig with everything requests need computed up front.
type rateLimit struct {
	rate           Rate
	limit          string // RateLimit-Limit value
	key            func(*operation.Context) (string, error)
	prefix         string
	failOpen       bool
	disableHeaders bool
}

func newRateLimit(cfg RateLimitConfig) (*rateLimit, error) {
	c := cfg.Sanitize()
	if c.Limit < 0 || c.Period < 0 || c.Burst < 0 {
		return nil, errors.New("http: rate limit: Limit, Period and Burst cannot be negative")
	}

	key := c.Key
	if key == nil {
		proxies := slices.Clone(c.TrustedProxies)
		key = func(ctx *operation.Context) (string, error) {
			return clientKey(ctx.State.(*State).Request, proxies), nil
		}
	}

	return &rateLimit{
		rate:           Rate{Limit: c.Limit, Period: c.Period, Burst: c.Burst},
		limit:          strconv.Itoa(c.Burst),
		key:            key,
		prefix:         c.Prefix,
		failOpen:       c.FailOpen,
		disableHeaders: c.DisableHeaders,
	}, nil
}

// middleware returns the middleware counting in l.
func (rl *rateLimit) middleware(l IRateLimiter) operation.Middleware {
	check := func(ctx *operation.Context) (bool, error) {
		k, err := rl.key(ctx)
		if err != nil {
			return false, err
		}
		if k == "" {
			return true, nil
		}

		res, err := l.Allow(ctx, rl.prefix+k, rl.rate)
		if err != nil {
			if !rl.failOpen {
				return false, ErrRateLimitUnavailable.Wrap(err)
			}
			logger.FromCtx(ctx).Error().
				Err(err).
				Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
				Msg("rate limiter failed, request admitted")
			return true, nil
		}

		h := ctx.State.(*State).Writer.Header()
		if !rl.disableHeaders {
			h.Set(rateLimitLimitHeader, rl.limit)
			h.Set(rateLimitRemainingHeader, strconv.Itoa(res.Remaining))
			h.Set(rateLimitResetHeader, ceilSeconds(res.ResetAfter))
		}
		if !res.Allowed {
			h.Set(retryAfterHeader, ceilSeconds(max(res.RetryAfter, time.Second)))
		}
		return res.Allowed, nil
	}

	return middleware.Guard(check, rejectRateLimited)
}

func rejectRateLimited(*operation.Context) error {
	return ErrRateLimited
}

// ceilSeconds formats d as whole seconds, rounded up.
func ceilSeconds(d time.Duration) string {
	return strconv.FormatInt(int64((d+time.Second-1)/time.Second), 10)
}

// JWTConfig configures JWTAuth.
type JWTConfig struct {
	// Keys verify token signatures. Required: JWTKeys for keys known up front, a *JWKS for an
	// identity provider's key set URL, or any IJWTKeySet.
	Keys IJWTKeySet
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

// JWTAuth returns a constructor, for forgo.MiddlewareAs, of a middleware that authenticates
// requests with a JSON Web Token sent as "Authorization: Bearer <token>".
//
// Tokens are parsed and verified by github.com/golang-jwt/jwt/v5, with keys looked up in Keys.
// The token must use one of HS256, HS384, HS512, RS256, RS384, RS512, PS256, PS384, PS512, ES256,
// ES384, ES512 or EdDSA. "none" is never accepted, and every algorithm only accepts keys of its
// own type and strength. The token must have an exp claim; exp, nbf, iss and aud are validated.
// Tokens with crit headers are rejected.
//
// A request without a token fails with ErrUnauthenticated, one with an invalid token with
// ErrInvalidToken or ErrTokenExpired, all answered 401 with a WWW-Authenticate header.
// The verified claims are available downstream through JWTClaimsFromCtx.
//
// The config is validated and the verifier built once, here. Every route the constructor is
// attached to shares them, and an invalid config is reported when bootstrap calls the constructor.
func JWTAuth(cfg JWTConfig) func() (operation.Middleware, error) {
	mw, err := newJWTAuth(cfg)
	return func() (operation.Middleware, error) {
		return mw, err
	}
}

func newJWTAuth(c JWTConfig) (operation.Middleware, error) {
	if c.Keys == nil {
		return nil, errors.New("http: jwt: Keys is required")
	}
	if v, ok := c.Keys.(interface{ validate() error }); ok {
		if err := v.validate(); err != nil {
			return nil, fmt.Errorf("http: jwt: %w", err)
		}
	}
	if c.Leeway < 0 {
		return nil, errors.New("http: jwt: Leeway cannot be negative")
	}

	v := newJWTVerifier(c.Keys, c, time.Now)

	check := func(ctx *operation.Context) (bool, error) {
		st := ctx.State.(*State)
		token := bearerToken(st.Request, c.Cookie)
		if token == "" {
			if c.Optional {
				return true, nil
			}
			st.Writer.Header().Set(wwwAuthenticateHeader, bearerChallenge)
			return false, nil
		}

		claims, err := v.verify(ctx, token)
		if err != nil {
			if errors.Is(err, errJWTKeySet) {
				return false, ErrAuthUnavailable.Wrap(err)
			}
			st.Writer.Header().Set(wwwAuthenticateHeader, invalidBearerHeader)
			if errors.Is(err, jwt.ErrTokenExpired) {
				return false, ErrTokenExpired.Wrap(err)
			}
			return false, ErrInvalidToken.Wrap(err)
		}

		ctx.Context = context.WithValue(ctx.Context, jwtClaimsKey{}, claims)
		return true, nil
	}

	return middleware.Guard(check, rejectUnauthenticated), nil
}

func rejectUnauthenticated(*operation.Context) error {
	return ErrUnauthenticated
}

// bearerToken returns the bearer token of the Authorization header, or else the value of the cookie.
func bearerToken(r *http.Request, cookie string) string {
	if scheme, token, ok := strings.Cut(r.Header.Get(authorizationHeader), " "); ok && strings.EqualFold(scheme, "Bearer") {
		if token = strings.TrimSpace(token); token != "" {
			return token
		}
	}
	if cookie != "" {
		if c, err := r.Cookie(cookie); err == nil {
			return c.Value
		}
	}
	return ""
}

type jwtClaimsKey struct{}

// JWTClaimsFromCtx returns the claims JWTAuth verified for the operation ctx belongs to.
// It reports false when the operation has no verified token.
func JWTClaimsFromCtx(ctx context.Context) (*JWTClaims, bool) {
	c, ok := ctx.Value(jwtClaimsKey{}).(*JWTClaims)
	return c, ok
}
