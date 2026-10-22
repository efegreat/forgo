// Package ratelimit admits operations at a rate, whatever their transport. A Rule turns an operation into
// a key and counts it in an ILimiter: Memory in the process, or a limiter backed by a store that every
// instance of the application shares, such as forgo-adapter's Redis one.
//
// The servers wrap it in middlewares that key operations by their client and answer the ones over the
// rate in their own protocol: http.RateLimit with 429 and RateLimit headers, grpc.RateLimit with
// ResourceExhausted and the same names as metadata.
//
// See [Rate limiting] for HTTP and [Authentication and rate limiting] for gRPC in REFERENCE.md.
//
// [Rate limiting]: https://github.com/efegreat/forgo/blob/main/REFERENCE.md#rate-limiting
// [Authentication and rate limiting]: https://github.com/efegreat/forgo/blob/main/REFERENCE.md#authentication-and-rate-limiting
package ratelimit

import (
	"errors"
	"net/netip"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

const (
	defaultLimit  = 60
	defaultPeriod = time.Minute
)

var (
	// ErrLimited rejects operations over the rate of a Rule.
	ErrLimited = operation.Error{
		Code:     "rate_limited",
		Status:   operation.StatusRateLimited,
		Message:  "too many requests",
		Severity: operation.SeverityLight,
	}
	// ErrUnavailable fails operations when the limiter fails and FailOpen is off.
	ErrUnavailable = operation.Error{
		Code:     "rate_limit_unavailable",
		Status:   operation.StatusUnavailable,
		Message:  "rate limiting unavailable",
		Severity: operation.SeverityCritical,
	}
)

// Config configures a Rule. The servers' RateLimit configs hold the same fields, and a default Key of
// their own. The zero value admits 60 operations per minute under each key.
type Config struct {
	// Limit is the number of operations admitted per Period. Default: 60.
	Limit int
	// Period is the time Limit applies to. Default: 1 minute.
	Period time.Duration
	// Burst is how many operations may arrive at once. Operations are refilled continuously, one every
	// Period/Limit. Default: Limit.
	Burst int
	// Key returns the key operations are counted under. An empty key exempts the operation. Required.
	Key func(*operation.Context) (string, error)
	// Prefix is prepended to every key. Give each Rule sharing a limiter its own Prefix, or they count into
	// the same buckets.
	Prefix string
	// FailOpen admits operations when the limiter fails, logging the failure. By default those operations
	// fail with ErrUnavailable.
	FailOpen bool
}

// Sanitize returns a copy of c with defaults applied.
func (c Config) Sanitize() Config {
	if c.Limit == 0 {
		c.Limit = defaultLimit
	}
	if c.Period == 0 {
		c.Period = defaultPeriod
	}
	if c.Burst == 0 {
		c.Burst = c.Limit
	}
	return c
}

// Rule admits operations at the rate of a sanitized Config. It is safe for concurrent use, and built once.
type Rule struct {
	rate     Rate
	key      func(*operation.Context) (string, error)
	prefix   string
	failOpen bool
}

// NewRule validates c, with its defaults applied, and returns its Rule.
func NewRule(cfg Config) (*Rule, error) {
	c := cfg.Sanitize()
	if c.Limit < 0 || c.Period < 0 || c.Burst < 0 {
		return nil, errors.New("ratelimit: Limit, Period and Burst cannot be negative")
	}
	if c.Key == nil {
		return nil, errors.New("ratelimit: Key is required")
	}
	return &Rule{
		rate:     Rate{Limit: c.Limit, Period: c.Period, Burst: c.Burst},
		key:      c.Key,
		prefix:   c.Prefix,
		failOpen: c.FailOpen,
	}, nil
}

// Rate returns the rate of r, with its defaults applied.
func (r *Rule) Rate() Rate {
	return r.rate
}

// Admit counts the operation ctx in l, under its key with the Prefix, and returns the result. counted
// reports whether the operation was counted, so that a transport only sends the rate limit information of
// counted ones: an empty key exempts the operation, and with FailOpen a failure of l admits it uncounted,
// logged. Without FailOpen, a failure of l is ErrUnavailable. An operation over the rate is no error: its
// result is not Allowed, and the transport answers ErrLimited once it sent when to retry.
func (r *Rule) Admit(ctx *operation.Context, l ILimiter) (res Result, counted bool, err error) {
	k, err := r.key(ctx)
	if err != nil {
		return Result{}, false, err
	}
	if k == "" {
		return Result{Allowed: true}, false, nil
	}
	res, err = l.Allow(ctx, r.prefix+k, r.rate)
	if err != nil {
		if !r.failOpen {
			return Result{}, false, ErrUnavailable.Wrap(err)
		}
		logger.FromCtx(ctx).Error().
			Err(err).
			Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
			Msg("rate limiter failed, request admitted")
		return Result{Allowed: true}, false, nil
	}
	return res, true, nil
}

// ClientKey returns the key that counts the operations of the client at ip: its address, or for IPv6 its
// /64 network, the smallest block usually assigned to one subscriber, so that a client cannot dodge its
// limit by rotating addresses inside its own block. An IPv4-mapped IPv6 address is keyed as IPv4. It
// returns "" for the zero netip.Addr.
func ClientKey(ip netip.Addr) string {
	ip = ip.Unmap()
	switch {
	case !ip.IsValid():
		return ""
	case ip.Is6():
		network, _ := ip.Prefix(64) // cannot fail: 64 bits fit an IPv6 address
		return network.String()
	default:
		return ip.String()
	}
}
