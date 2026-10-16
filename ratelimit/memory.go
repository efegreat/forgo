package ratelimit

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"time"
)

const (
	defaultMaxKeys = 1 << 20
	shards         = 64
	sweepInterval  = time.Minute
)

var errInvalidRate = errors.New("ratelimit: limit and period must be positive")

// Rate is the rate operations under one key are admitted at: Limit operations per Period, of which up to
// Burst may arrive at once. A Burst of zero or less means Limit.
type Rate struct {
	Limit  int
	Period time.Duration
	Burst  int
}

// Result is the outcome of one admission.
type Result struct {
	// Allowed reports whether the operation is admitted.
	Allowed bool
	// Remaining is how many more operations would be admitted right now.
	Remaining int
	// RetryAfter is how long a rejected operation has to wait before one is admitted again.
	RetryAfter time.Duration
	// ResetAfter is how long it takes until the full burst is available again.
	ResetAfter time.Duration
}

// ILimiter admits operations under a key at a rate.
//
// The rate is passed on every call, so one limiter serves any number of RateLimit middlewares, of any
// transport. Implementations must be safe for concurrent use. A limiter backed by a shared store enforces
// the rate across every instance of the application; Memory only within one process.
type ILimiter interface {
	Allow(ctx context.Context, key string, rate Rate) (Result, error)
}

// Memory is an in-process ILimiter using the generic cell rate algorithm (GCRA),
// which behaves like a token bucket refilled continuously while storing a single timestamp per key.
//
// Keys whose burst has fully recovered hold no information and are swept. The number of tracked
// keys is bounded: once full, admitting a new key evicts an arbitrary one, which resets that key's
// limit instead of letting memory grow without bound.
type Memory struct {
	seed      maphash.Seed
	epoch     time.Time
	shardKeys int
	clock     func() time.Duration // elapsed time since epoch; replaced in tests
	shards    [shards]rateShard
}

type rateShard struct {
	mu sync.Mutex
	// tats holds the theoretical arrival time of the next request of every key, as time since epoch.
	tats      map[string]time.Duration
	nextSweep time.Duration
}

// NewMemory returns a Memory that tracks up to maxKeys keys.
// Zero or less means 1 << 20 keys.
func NewMemory(maxKeys int) *Memory {
	if maxKeys <= 0 {
		maxKeys = defaultMaxKeys
	}
	l := &Memory{
		seed:      maphash.MakeSeed(),
		epoch:     time.Now(),
		shardKeys: max(1, (maxKeys+shards-1)/shards),
	}
	for i := range l.shards {
		l.shards[i].tats = make(map[string]time.Duration)
	}
	return l
}

// Allow admits a request under key at rate. It fails only for a rate whose Limit or Period is not positive.
func (l *Memory) Allow(_ context.Context, key string, rate Rate) (Result, error) {
	if rate.Limit <= 0 || rate.Period <= 0 {
		return Result{}, errInvalidRate
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
	s := &l.shards[maphash.String(l.seed, key)%shards]

	s.mu.Lock()
	defer s.mu.Unlock()

	if now >= s.nextSweep {
		s.sweep(now)
		s.nextSweep = now + sweepInterval
	}

	tat, tracked := s.tats[key]
	tat = max(tat, now)
	next := tat + emission
	diff := now - (next - burstOffset)
	if diff < 0 {
		return Result{RetryAfter: -diff, ResetAfter: tat - now}, nil
	}

	if !tracked && len(s.tats) >= l.shardKeys {
		s.evictOne()
	}
	s.tats[key] = next

	return Result{
		Allowed:    true,
		Remaining:  int(diff / emission),
		ResetAfter: next - now,
	}, nil
}

func (l *Memory) now() time.Duration {
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
