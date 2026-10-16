package grpc

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"time"

	"github.com/efegreat/forgo/jwtauth"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/ratelimit"
	"github.com/efegreat/forgo/transport"

	"google.golang.org/grpc/metadata"
)

const (
	authorizationMetadata      = "authorization"
	forwardedForMetadata       = "x-forwarded-for"
	retryAfterMetadata         = "retry-after"
	rateLimitLimitMetadata     = "ratelimit-limit"
	rateLimitRemainingMetadata = "ratelimit-remaining"
	rateLimitResetMetadata     = "ratelimit-reset"

	// unknownPeerKey counts the calls of a client grpc-go knows no address of, together, rather than
	// exempting them.
	unknownPeerKey = "unknown"
)

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
	// Optional lets calls without a token through, without claims. A token that is sent must still be
	// valid.
	Optional bool
}

// JWTAuth returns a constructor, for forgo.MiddlewareAs, of a middleware that authenticates calls with a
// JSON Web Token sent in the authorization metadata as "Bearer <token>", as grpc-go's per-RPC
// credentials, such as oauth.TokenSource, send it. A jwtauth.Authenticator verifies it (see jwtauth.New
// for the rules), once per call, when it starts: a stream is authenticated before its handler runs.
//
// A call without a token fails with jwtauth.ErrUnauthenticated, one with an invalid token with
// jwtauth.ErrInvalidToken or jwtauth.ErrTokenExpired, all answered Unauthenticated. A call whose keys
// cannot be looked up fails with jwtauth.ErrAuthUnavailable, answered Unavailable. The verified claims are
// available to the handler through jwtauth.ClaimsFromCtx.
//
// Attached to the server, it also guards HealthService and the reflection services, which probes and
// tools call without a token. Attach it to the services that need it instead.
//
// The config is validated and the verifier built once, here. Every method the constructor is attached to
// shares them, and an invalid config is reported when bootstrap calls the constructor.
func JWTAuth(cfg JWTConfig) func() (operation.Middleware, error) {
	mw, err := newJWTAuth(cfg)
	return func() (operation.Middleware, error) {
		return mw, err
	}
}

func newJWTAuth(c JWTConfig) (operation.Middleware, error) {
	a, err := jwtauth.New(jwtauth.Config{
		Keys: c.Keys, Issuer: c.Issuer, Audience: c.Audience, Leeway: c.Leeway, Optional: c.Optional,
	})
	if err != nil {
		return nil, fmt.Errorf("grpc: %w", err)
	}
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if err := a.Authenticate(ctx, bearerToken(ctx.State.(*State))); err != nil {
				return err
			}
			return next(ctx)
		}
	}, nil
}

// bearerToken returns the bearer token of the first authorization metadata value of the call.
func bearerToken(st *State) string {
	values := metadata.ValueFromIncomingContext(st.stream.Context(), authorizationMetadata)
	if len(values) == 0 {
		return ""
	}
	return jwtauth.BearerToken(values[0])
}

// RateLimitConfig configures RateLimit. The zero value admits 60 calls per minute per client IP.
type RateLimitConfig struct {
	// Limit is the number of calls admitted per Period. Default: 60.
	Limit int
	// Period is the time Limit applies to. Default: 1 minute.
	Period time.Duration
	// Burst is how many calls may arrive at once. Calls are refilled continuously, one every
	// Period/Limit. Default: Limit.
	Burst int
	// Key returns the key calls are counted under. An empty key exempts the call. Default: the client
	// IP, with IPv6 clients counted by their /64 network, and the clients grpc-go knows no IP of
	// together.
	Key func(*operation.Context) (string, error)
	// TrustedProxies are the networks of the proxies in front of the server. The default Key reads the
	// client IP from the x-forwarded-for metadata only behind them, as http.ClientIP reads the header.
	// Without them the metadata is ignored, because any client can send it.
	TrustedProxies []netip.Prefix
	// Prefix is prepended to every key. Give each RateLimitWith middleware sharing a limiter its own
	// Prefix, or they count into the same buckets.
	Prefix string
	// FailOpen admits calls when the limiter fails, logging the failure. By default those calls fail with
	// ratelimit.ErrUnavailable.
	FailOpen bool
	// DisableMetadata stops the ratelimit-limit, ratelimit-remaining and ratelimit-reset header metadata.
	// Rejected calls always get retry-after.
	DisableMetadata bool
}

// Sanitize returns a copy of c with defaults applied.
func (c RateLimitConfig) Sanitize() RateLimitConfig {
	r := ratelimit.Config{Limit: c.Limit, Period: c.Period, Burst: c.Burst}.Sanitize()
	c.Limit, c.Period, c.Burst = r.Limit, r.Period, r.Burst
	return c
}

// RateLimit returns a constructor, for forgo.MiddlewareAs, of a middleware that rejects calls over the
// configured rate with ratelimit.ErrLimited, answered ResourceExhausted, and the retry-after header
// metadata: the seconds until a call is admitted again. Admitted calls get the ratelimit-limit,
// ratelimit-remaining and ratelimit-reset header metadata, as http.RateLimit sends its headers. A stream
// counts once, when it starts.
//
// Each middleware it constructs counts in its own ratelimit.Memory: attached to the server, the rate
// applies to all methods together; attached to a service or a method, to that one alone. The counts are
// per process. To share them between instances, use RateLimitWith.
func RateLimit(cfg RateLimitConfig) func() (operation.Middleware, error) {
	rl, err := newRateLimit(cfg)
	return func() (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		return rl.middleware(ratelimit.NewMemory(0)), nil
	}
}

// RateLimitWith is RateLimit counting in a ratelimit.ILimiter resolved by bootstrap, such as one backed by
// a store shared between instances. An HTTP server's RateLimitWith may share it, with another Prefix.
func RateLimitWith(cfg RateLimitConfig) func(ratelimit.ILimiter) (operation.Middleware, error) {
	rl, err := newRateLimit(cfg)
	return func(l ratelimit.ILimiter) (operation.Middleware, error) {
		if err != nil {
			return nil, err
		}
		if l == nil {
			return nil, errors.New("grpc: rate limit: nil limiter")
		}
		return rl.middleware(l), nil
	}
}

// rateLimit is the rule of a RateLimitConfig, with what responses need computed up front.
type rateLimit struct {
	rule            *ratelimit.Rule
	limit           string // ratelimit-limit value
	disableMetadata bool
}

func newRateLimit(c RateLimitConfig) (*rateLimit, error) {
	key := c.Key
	if key == nil {
		proxies := slices.Clone(c.TrustedProxies)
		key = func(ctx *operation.Context) (string, error) {
			return clientKey(ctx.State.(*State), proxies), nil
		}
	}
	rule, err := ratelimit.NewRule(ratelimit.Config{
		Limit: c.Limit, Period: c.Period, Burst: c.Burst, Key: key, Prefix: c.Prefix, FailOpen: c.FailOpen,
	})
	if err != nil {
		return nil, fmt.Errorf("grpc: rate limit: %w", err)
	}
	return &rateLimit{rule: rule, limit: strconv.Itoa(rule.Rate().Burst), disableMetadata: c.DisableMetadata}, nil
}

// middleware returns the middleware counting in l.
func (rl *rateLimit) middleware(l ratelimit.ILimiter) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			res, counted, err := rl.rule.Admit(ctx, l)
			if err != nil {
				return err
			}
			if !counted {
				return next(ctx)
			}
			var md metadata.MD
			if !rl.disableMetadata {
				md = metadata.MD{
					rateLimitLimitMetadata:     {rl.limit},
					rateLimitRemainingMetadata: {strconv.Itoa(res.Remaining)},
					rateLimitResetMetadata:     {ceilSeconds(res.ResetAfter)},
				}
			}
			if !res.Allowed {
				if md == nil {
					md = metadata.MD{}
				}
				md[retryAfterMetadata] = []string{ceilSeconds(max(res.RetryAfter, time.Second))}
			}
			if md != nil {
				// The header is only sent by the handler, which has not run yet.
				_ = ctx.State.(*State).SetHeader(md)
			}
			if !res.Allowed {
				return ratelimit.ErrLimited
			}
			return next(ctx)
		}
	}
}

// clientKey returns the rate limit key of the client of the call, as ratelimit.ClientKey does: the IP of
// its peer, or the one the x-forwarded-for metadata names behind trustedProxies. A peer that is not an IP
// address is keyed by its address, and an unknown one by unknownPeerKey.
func clientKey(st *State, trustedProxies []netip.Prefix) string {
	p, ok := st.Peer()
	if !ok || p.Addr == nil {
		return unknownPeerKey
	}
	ip := transport.ClientIP(peerIP(p.Addr), func() []string {
		return metadata.ValueFromIncomingContext(st.stream.Context(), forwardedForMetadata)
	}, trustedProxies)
	if k := ratelimit.ClientKey(ip); k != "" {
		return k
	}
	return p.Addr.String()
}

// peerIP returns the IP address of addr, without the allocations of its String for a TCP address, or the
// zero netip.Addr when it has none.
func peerIP(addr net.Addr) netip.Addr {
	if a, ok := addr.(*net.TCPAddr); ok {
		ip, _ := netip.AddrFromSlice(a.IP)
		return ip.Unmap()
	}
	return transport.ParseIP(addr.String())
}

// ceilSeconds formats d as whole seconds, rounded up.
func ceilSeconds(d time.Duration) string {
	return strconv.FormatInt(int64((d+time.Second-1)/time.Second), 10)
}
