package http

import (
	"context"
	"errors"
	"hash/maphash"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
)

const (
	defaultRateLimiterMaxKeys = 1 << 20
	rateLimiterShards         = 64
	rateLimiterSweepInterval  = time.Minute
	forwardedForHeader        = "X-Forwarded-For"
)

var errInvalidRate = errors.New("rate limiter: limit and period must be positive")

// Rate is the rate requests under one key are admitted at: Limit requests per Period,
// of which up to Burst may arrive at once. A Burst of zero or less means Limit.
type Rate struct {
	Limit  int
	Period time.Duration
	Burst  int
}

// RateLimitResult is the outcome of one admission.
type RateLimitResult struct {
	// Allowed reports whether the request is admitted.
	Allowed bool
	// Remaining is how many more requests would be admitted right now.
	Remaining int
	// RetryAfter is how long a rejected request has to wait before one is admitted again.
	RetryAfter time.Duration
	// ResetAfter is how long it takes until the full burst is available again.
	ResetAfter time.Duration
}

// IRateLimiter admits requests under a key at a rate.
//
// The rate is passed on every call, so one limiter serves any number of RateLimit middlewares.
// Implementations must be safe for concurrent use. A limiter backed by a shared store enforces
// the rate across every instance of the application; MemoryRateLimiter only within one process.
type IRateLimiter interface {
	Allow(ctx context.Context, key string, rate Rate) (RateLimitResult, error)
}

// MemoryRateLimiter is an in-process IRateLimiter using the generic cell rate algorithm (GCRA),
// which behaves like a token bucket refilled continuously while storing a single timestamp per key.
//
// Keys whose burst has fully recovered hold no information and are swept. The number of tracked
// keys is bounded: once full, admitting a new key evicts an arbitrary one, which resets that key's
// limit instead of letting memory grow without bound.
type MemoryRateLimiter struct {
	seed      maphash.Seed
	epoch     time.Time
	shardKeys int
	clock     func() time.Duration // elapsed time since epoch; replaced in tests
	shards    [rateLimiterShards]rateShard
}

type rateShard struct {
	mu sync.Mutex
	// tats holds the theoretical arrival time of the next request of every key, as time since epoch.
	tats      map[string]time.Duration
	nextSweep time.Duration
}

// NewMemoryRateLimiter returns a MemoryRateLimiter that tracks up to maxKeys keys.
// Zero or less means 1 << 20 keys.
func NewMemoryRateLimiter(maxKeys int) *MemoryRateLimiter {
	if maxKeys <= 0 {
		maxKeys = defaultRateLimiterMaxKeys
	}
	l := &MemoryRateLimiter{
		seed:      maphash.MakeSeed(),
		epoch:     time.Now(),
		shardKeys: max(1, (maxKeys+rateLimiterShards-1)/rateLimiterShards),
	}
	for i := range l.shards {
		l.shards[i].tats = make(map[string]time.Duration)
	}
	return l
}

// Allow admits a request under key at rate. It fails only for a rate whose Limit or Period is not positive.
func (l *MemoryRateLimiter) Allow(_ context.Context, key string, rate Rate) (RateLimitResult, error) {
	if rate.Limit <= 0 || rate.Period <= 0 {
		return RateLimitResult{}, errInvalidRate
	}
	burst := rate.Burst
	if burst <= 0 {
		burst = rate.Limit
	}
	// One request is admitted every emission interval; burstOffset is how far ahead of now
	// the theoretical arrival time may run before requests are rejected.
	emission := max(rate.Period/time.Duration(rate.Limit), 1)
	burstOffset := emission * time.Duration(burst)

	now := l.now()
	s := &l.shards[maphash.String(l.seed, key)%rateLimiterShards]

	s.mu.Lock()
	defer s.mu.Unlock()

	if now >= s.nextSweep {
		s.sweep(now)
		s.nextSweep = now + rateLimiterSweepInterval
	}

	tat, tracked := s.tats[key]
	tat = max(tat, now)
	next := tat + emission
	diff := now - (next - burstOffset)
	if diff < 0 {
		return RateLimitResult{RetryAfter: -diff, ResetAfter: tat - now}, nil
	}

	if !tracked && len(s.tats) >= l.shardKeys {
		s.evictOne()
	}
	s.tats[key] = next

	return RateLimitResult{
		Allowed:    true,
		Remaining:  int(diff / emission),
		ResetAfter: next - now,
	}, nil
}

func (l *MemoryRateLimiter) now() time.Duration {
	if l.clock != nil {
		return l.clock()
	}
	return time.Since(l.epoch)
}

// sweep drops the keys whose burst has fully recovered: they behave exactly like untracked keys.
func (s *rateShard) sweep(now time.Duration) {
	for key, tat := range s.tats {
		if tat <= now {
			delete(s.tats, key)
		}
	}
}

func (s *rateShard) evictOne() {
	for key := range s.tats {
		delete(s.tats, key)
		return
	}
}

// ClientIP returns the address of the client that sent r.
//
// It is the peer address, unless the peer is one of trustedProxies. Then X-Forwarded-For is read
// from right to left and the first address that is not a trusted proxy is the client. Addresses
// left of it were written by the client and are ignored, so the header cannot be spoofed past the
// proxies you trust. With no trusted proxies the header is never read.
//
// The zero netip.Addr is returned when the peer address is not an IP address, as with Unix sockets.
func ClientIP(r *http.Request, trustedProxies []netip.Prefix) netip.Addr {
	ip := parseIP(r.RemoteAddr)
	if !ip.IsValid() || !trusted(ip, trustedProxies) {
		return ip
	}

	values := r.Header.Values(forwardedForHeader)
	for i := len(values) - 1; i >= 0; i-- {
		hops := values[i]
		for hops != "" {
			hop := hops
			if j := strings.LastIndexByte(hops, ','); j >= 0 {
				hop, hops = hops[j+1:], hops[:j]
			} else {
				hops = ""
			}
			addr := parseIP(strings.TrimSpace(hop))
			if !addr.IsValid() {
				// The chain cannot be followed further: the last trusted hop is all that is known.
				return ip
			}
			if !trusted(addr, trustedProxies) {
				return addr
			}
			ip = addr
		}
	}
	return ip
}

// parseIP parses an address with or without a port. IPv4-mapped IPv6 addresses are unmapped.
func parseIP(s string) netip.Addr {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap()
	}
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.Unmap()
	}
	return netip.Addr{}
}

func trusted(ip netip.Addr, proxies []netip.Prefix) bool {
	for _, p := range proxies {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// clientKey returns the rate limit key of the client that sent r. IPv6 clients are keyed by their
// /64 network, the smallest block usually assigned to one subscriber, so a client cannot dodge its
// limit by rotating addresses inside its own block.
func clientKey(r *http.Request, trustedProxies []netip.Prefix) string {
	ip := ClientIP(r, trustedProxies)
	switch {
	case !ip.IsValid():
		return r.RemoteAddr
	case ip.Is6():
		network, _ := ip.Prefix(64) // cannot fail: 64 bits fit an IPv6 address
		return network.String()
	default:
		return ip.String()
	}
}
