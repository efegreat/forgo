package ratelimit

import (
	"context"
	"errors"
	"hash/maphash"
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

func newTestLimiter(maxKeys int) (*Memory, *fakeClock) {
	l := NewMemory(maxKeys)
	c := &fakeClock{}
	l.clock = c.elapsed
	return l, c
}

func (l *Memory) trackedKeys() int {
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
		want := Result{Allowed: s.allowed, Remaining: s.remaining, RetryAfter: s.retryAfter, ResetAfter: s.resetAfter}
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
	l := NewMemory(0)
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
	clock.advance(sweepInterval)
	var probed [shards]bool
	for i, n := 0, 0; n < shards; i++ {
		key := "probe" + strconv.Itoa(i)
		if shard := maphash.String(l.seed, key) % shards; !probed[shard] {
			probed[shard] = true
			n++
			_, _ = l.Allow(context.Background(), key, rate)
		}
	}
	if n := l.trackedKeys(); n != shards {
		t.Fatalf("tracked %d keys after sweep, want %d", n, shards)
	}
}

func TestMemoryRateLimiterBoundsKeys(t *testing.T) {
	l, _ := newTestLimiter(shards) // one key per shard
	rate := Rate{Limit: 1, Period: time.Hour}
	for i := range 10_000 {
		if got, _ := l.Allow(context.Background(), strconv.Itoa(i), rate); !got.Allowed {
			t.Fatalf("new key %d rejected", i)
		}
	}
	if n := l.trackedKeys(); n > shards {
		t.Fatalf("tracked %d keys, limit %d", n, shards)
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
