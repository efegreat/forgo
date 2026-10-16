package ratelimit

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
)

// limiterFunc is an ILimiter of a function.
type limiterFunc func(context.Context, string, Rate) (Result, error)

func (f limiterFunc) Allow(ctx context.Context, key string, rate Rate) (Result, error) {
	return f(ctx, key, rate)
}

func TestRuleAdmit(t *testing.T) {
	op := &operation.Context{Context: context.Background()}
	staticKey := func(k string) func(*operation.Context) (string, error) {
		return func(*operation.Context) (string, error) { return k, nil }
	}
	var gotKey string
	var gotRate Rate
	allow := limiterFunc(func(_ context.Context, key string, rate Rate) (Result, error) {
		gotKey, gotRate = key, rate
		return Result{Allowed: key != "p:full", Remaining: 3}, nil
	})
	failing := limiterFunc(func(context.Context, string, Rate) (Result, error) {
		return Result{}, errors.New("redis: connection refused")
	})
	errKey := errors.New("no tenant")

	for _, tc := range []struct {
		name        string
		cfg         Config
		l           ILimiter
		allowed     bool
		counted     bool
		wantErr     error
		wantLimiter string
	}{
		{"counted", Config{Key: staticKey("a"), Prefix: "p:"}, allow, true, true, nil, "p:a"},
		{"over the rate", Config{Key: staticKey("full"), Prefix: "p:"}, allow, false, true, nil, "p:full"},
		{"exempt", Config{Key: staticKey("")}, allow, true, false, nil, ""},
		{"key error", Config{Key: func(*operation.Context) (string, error) { return "", errKey }}, allow, false, false, errKey, ""},
		{"limiter fails", Config{Key: staticKey("a")}, failing, false, false, ErrUnavailable, ""},
		{"limiter fails open", Config{Key: staticKey("a"), FailOpen: true}, failing, true, false, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotKey = ""
			r, err := NewRule(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			res, counted, err := r.Admit(op, tc.l)
			if !errors.Is(err, tc.wantErr) || (err == nil) != (tc.wantErr == nil) {
				t.Fatalf("err %v, want %v", err, tc.wantErr)
			}
			if err == nil && (res.Allowed != tc.allowed || counted != tc.counted) {
				t.Fatalf("allowed %v, counted %v", res.Allowed, counted)
			}
			if gotKey != tc.wantLimiter {
				t.Fatalf("limiter key %q, want %q", gotKey, tc.wantLimiter)
			}
		})
	}
	if want := (Rate{Limit: 60, Period: time.Minute, Burst: 60}); gotRate != want {
		t.Fatalf("rate %+v, want the defaults %+v", gotRate, want)
	}
}

func TestNewRuleValidates(t *testing.T) {
	key := func(*operation.Context) (string, error) { return "k", nil }
	for name, c := range map[string]Config{
		"no key":          {},
		"negative limit":  {Key: key, Limit: -1},
		"negative burst":  {Key: key, Burst: -1},
		"negative period": {Key: key, Period: -time.Second},
	} {
		if _, err := NewRule(c); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	r, err := NewRule(Config{Key: key, Limit: 5, Period: time.Second})
	if err != nil || r.Rate() != (Rate{Limit: 5, Period: time.Second, Burst: 5}) {
		t.Fatalf("%+v, %v", r, err)
	}
}

func TestClientKey(t *testing.T) {
	for in, want := range map[string]string{
		"203.0.113.7":          "203.0.113.7",
		"2001:db8:1:2:3:4:5:6": "2001:db8:1:2::/64",
		"::ffff:203.0.113.7":   "203.0.113.7",
	} {
		if got := ClientKey(netip.MustParseAddr(in)); got != want {
			t.Errorf("ClientKey(%s) = %q, want %q", in, got, want)
		}
	}
	if got := ClientKey(netip.Addr{}); got != "" {
		t.Errorf("ClientKey of no address = %q", got)
	}
}
