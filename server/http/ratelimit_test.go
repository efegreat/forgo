package http

import (
	"context"
	"errors"
	"hash/maphash"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a clock tests move by hand.
type fakeClock struct{ now atomic.Int64 }

func (c *fakeClock) elapsed() time.Duration  { return time.Duration(c.now.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.now.Add(int64(d)) }

func newTestLimiter(maxKeys int) (*MemoryRateLimiter, *fakeClock) {
	l := NewMemoryRateLimiter(maxKeys)
	c := &fakeClock{}
	l.clock = c.elapsed
	return l, c
}

func (l *MemoryRateLimiter) trackedKeys() int {
	n := 0
	for i := range l.shards {
		l.shards[i].mu.Lock()
		n += len(l.shards[i].tats)
		l.shards[i].mu.Unlock()
	}
	return n
}

func TestMemoryRateLimiterGCRA(t *testing.T) {
	l, clock := newTestLimiter(0)
	ctx := context.Background()
	rate := Rate{Limit: 10, Period: 10 * time.Second} // one request per second, burst 10

	type step struct {
		advance    time.Duration
		allowed    bool
		remaining  int
		retryAfter time.Duration
		resetAfter time.Duration
	}
	steps := []step{}
	for i := range 10 {
		steps = append(steps, step{allowed: true, remaining: 9 - i, resetAfter: time.Duration(i+1) * time.Second})
	}
	steps = append(steps,
		step{allowed: false, retryAfter: time.Second, resetAfter: 10 * time.Second},
		step{advance: time.Second, allowed: true, remaining: 0, resetAfter: 10 * time.Second},
		step{advance: 400 * time.Millisecond, allowed: false, retryAfter: 600 * time.Millisecond, resetAfter: 9600 * time.Millisecond},
		step{advance: 5600 * time.Millisecond, allowed: true, remaining: 5, resetAfter: 5 * time.Second},
		step{advance: time.Hour, allowed: true, remaining: 9, resetAfter: time.Second},
	)

	for i, s := range steps {
		clock.advance(s.advance)
		got, err := l.Allow(ctx, "k", rate)
		if err != nil {
			t.Fatal(err)
		}
		want := RateLimitResult{Allowed: s.allowed, Remaining: s.remaining, RetryAfter: s.retryAfter, ResetAfter: s.resetAfter}
		if got != want {
			t.Fatalf("step %d: got %+v, want %+v", i, got, want)
		}
	}

	// Other keys have their own budget.
	if got, _ := l.Allow(ctx, "other", rate); !got.Allowed || got.Remaining != 9 {
		t.Fatalf("independent key: %+v", got)
	}
}

func TestMemoryRateLimiterBurst(t *testing.T) {
	l, clock := newTestLimiter(0)
	rate := Rate{Limit: 60, Period: time.Minute, Burst: 1}

	if got, _ := l.Allow(context.Background(), "k", rate); !got.Allowed || got.Remaining != 0 {
		t.Fatalf("first: %+v", got)
	}
	if got, _ := l.Allow(context.Background(), "k", rate); got.Allowed || got.RetryAfter != time.Second {
		t.Fatalf("second: %+v", got)
	}
	clock.advance(time.Second)
	if got, _ := l.Allow(context.Background(), "k", rate); !got.Allowed {
		t.Fatalf("after refill: %+v", got)
	}
}

func TestMemoryRateLimiterInvalidRate(t *testing.T) {
	l := NewMemoryRateLimiter(0)
	for _, rate := range []Rate{{}, {Limit: 1}, {Period: time.Second}, {Limit: -1, Period: time.Second}} {
		if _, err := l.Allow(context.Background(), "k", rate); !errors.Is(err, errInvalidRate) {
			t.Errorf("rate %+v: err %v", rate, err)
		}
	}
}

func TestMemoryRateLimiterSweepsRecoveredKeys(t *testing.T) {
	l, clock := newTestLimiter(0)
	rate := Rate{Limit: 1, Period: time.Second}
	for i := range 1000 {
		_, _ = l.Allow(context.Background(), strconv.Itoa(i), rate)
	}
	if n := l.trackedKeys(); n != 1000 {
		t.Fatalf("tracked %d keys, want 1000", n)
	}

	// Every key has recovered. The first call into each shard after the sweep interval clears it,
	// leaving only the probing key.
	clock.advance(rateLimiterSweepInterval)
	var probed [rateLimiterShards]bool
	for i, n := 0, 0; n < rateLimiterShards; i++ {
		key := "probe" + strconv.Itoa(i)
		if shard := maphash.String(l.seed, key) % rateLimiterShards; !probed[shard] {
			probed[shard] = true
			n++
			_, _ = l.Allow(context.Background(), key, rate)
		}
	}
	if n := l.trackedKeys(); n != rateLimiterShards {
		t.Fatalf("tracked %d keys after sweep, want %d", n, rateLimiterShards)
	}
}

func TestMemoryRateLimiterBoundsKeys(t *testing.T) {
	l, _ := newTestLimiter(rateLimiterShards) // one key per shard
	rate := Rate{Limit: 1, Period: time.Hour}
	for i := range 10_000 {
		if got, _ := l.Allow(context.Background(), strconv.Itoa(i), rate); !got.Allowed {
			t.Fatalf("new key %d rejected", i)
		}
	}
	if n := l.trackedKeys(); n > rateLimiterShards {
		t.Fatalf("tracked %d keys, limit %d", n, rateLimiterShards)
	}
}

func TestMemoryRateLimiterConcurrent(t *testing.T) {
	l, _ := newTestLimiter(0) // the clock stands still, so exactly the burst is admitted
	rate := Rate{Limit: 50, Period: time.Hour}

	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 10 {
				got, err := l.Allow(context.Background(), "shared", rate)
				if err != nil {
					t.Error(err)
					return
				}
				if got.Allowed {
					admitted.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if n := admitted.Load(); n != 50 {
		t.Fatalf("admitted %d, want 50", n)
	}
}

func TestClientIP(t *testing.T) {
	proxies := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}

	for _, tc := range []struct {
		name    string
		remote  string
		xff     []string
		trusted []netip.Prefix
		want    string
	}{
		{"peer v4", "203.0.113.7:5000", nil, nil, "203.0.113.7"},
		{"peer v6", "[2001:db8::1]:5000", nil, nil, "2001:db8::1"},
		{"peer v4-mapped", "[::ffff:203.0.113.7]:5000", nil, nil, "203.0.113.7"},
		{"peer without port", "203.0.113.7", nil, nil, "203.0.113.7"},
		{"unix socket", "@", nil, nil, "invalid IP"},
		{"header ignored without trusted proxies", "203.0.113.7:5000", []string{"198.51.100.1"}, nil, "203.0.113.7"},
		{"header ignored from untrusted peer", "203.0.113.7:5000", []string{"198.51.100.1"}, proxies, "203.0.113.7"},
		{"behind proxy", "10.0.0.2:80", []string{"198.51.100.1"}, proxies, "198.51.100.1"},
		{"spoofed entries left of the client", "10.0.0.2:80", []string{"1.1.1.1, 198.51.100.1"}, proxies, "198.51.100.1"},
		{"proxy chain", "10.0.0.2:80", []string{"198.51.100.1, 10.0.0.9", "10.0.0.3"}, proxies, "198.51.100.1"},
		{"hops with ports", "10.0.0.2:80", []string{"[2001:db8::5]:1234, 10.0.0.9:80"}, proxies, "2001:db8::5"},
		{"all trusted", "10.0.0.2:80", []string{"10.0.0.1"}, proxies, "10.0.0.1"},
		{"malformed hop", "10.0.0.2:80", []string{"garbage, 10.0.0.3"}, proxies, "10.0.0.3"},
		{"empty header", "10.0.0.2:80", nil, proxies, "10.0.0.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			r.RemoteAddr = tc.remote
			for _, v := range tc.xff {
				r.Header.Add(forwardedForHeader, v)
			}
			if got := ClientIP(r, tc.trusted).String(); got != tc.want {
				t.Fatalf("ClientIP = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestClientKey(t *testing.T) {
	for _, tc := range []struct{ remote, want string }{
		{"203.0.113.7:5000", "203.0.113.7"},
		{"[2001:db8:1:2:3:4:5:6]:5000", "2001:db8:1:2::/64"},
		{"[fe80::1%eth0]:5000", "fe80::/64"},
		{"@", "@"},
	} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = tc.remote
		if got := clientKey(r, nil); got != tc.want {
			t.Errorf("clientKey(%q) = %q, want %q", tc.remote, got, tc.want)
		}
	}
}
