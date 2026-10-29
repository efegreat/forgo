package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// failing returns a handler that fails its first n runs with "fail #<run>" and then succeeds.
func failing(n int, runs *int) operation.Handler {
	return func(*operation.Context) error {
		*runs++
		if *runs <= n {
			return fmt.Errorf("fail #%d", *runs)
		}
		return nil
	}
}

func TestTolerance(t *testing.T) {
	errBad := errors.New("bad input")

	for _, tc := range []struct {
		name        string
		conf        operation.Tolerance
		failures    int
		wantErr     string
		wantRuns    int
		wantRetries []string // "<attempt> <err>" per OnRetry call
		wantSuccess bool
		wantFailure string
	}{
		{"no retry", operation.Tolerance{RetryAttempts: 3}, 1, "fail #1", 1, nil, false, ""},
		{"first attempt succeeds", operation.Tolerance{Retry: true, RetryAttempts: 3}, 0, "", 1, nil, true, ""},
		{"succeeds on the third attempt", operation.Tolerance{Retry: true, RetryAttempts: 3}, 2, "", 3, []string{"1 fail #1", "2 fail #2"}, true, ""},
		{"every attempt fails", operation.Tolerance{Retry: true, RetryAttempts: 3}, 5, "fail #3", 3, []string{"1 fail #1", "2 fail #2"}, false, "fail #3"},
		{"one attempt", operation.Tolerance{Retry: true, RetryAttempts: 1}, 5, "fail #1", 1, nil, false, "fail #1"},
		{"unlimited attempts", operation.Tolerance{Retry: true}, 4, "", 5, []string{"1 fail #1", "2 fail #2", "3 fail #3", "4 fail #4"}, true, ""},
		{
			"RetryIf refuses", operation.Tolerance{Retry: true, RetryAttempts: 3, RetryIf: func(err error) bool { return err.Error() != "fail #2" }},
			5, "fail #2", 2, []string{"1 fail #1"}, false, "fail #2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			var retries []string
			success, failure := false, ""
			conf := tc.conf
			conf.OnRetry = func(_ *operation.Context, attempt int, err error) {
				retries = append(retries, fmt.Sprintf("%d %v", attempt, err))
			}
			conf.OnSuccess = func(*operation.Context) { success = true }
			conf.OnFailure = func(_ *operation.Context, err error) { failure = err.Error() }

			runs := 0
			err := Tolerance(conf)(failing(tc.failures, &runs))(newTestCtx(&out))
			if (err == nil) != (tc.wantErr == "") || (err != nil && err.Error() != tc.wantErr) {
				t.Fatalf("err %v, want %q", err, tc.wantErr)
			}
			if runs != tc.wantRuns {
				t.Fatalf("ran %d times, want %d", runs, tc.wantRuns)
			}
			if !slices.Equal(retries, tc.wantRetries) {
				t.Fatalf("OnRetry calls %q, want %q", retries, tc.wantRetries)
			}
			if success != tc.wantSuccess || failure != tc.wantFailure {
				t.Fatalf("OnSuccess %v, OnFailure %q, want %v and %q", success, failure, tc.wantSuccess, tc.wantFailure)
			}
		})
	}

	t.Run("error returned unchanged", func(t *testing.T) {
		var out bytes.Buffer
		conf := operation.Tolerance{Retry: true, RetryAttempts: 3, RetryIf: func(error) bool { return false }}
		err := Tolerance(conf)(func(*operation.Context) error { return errBad })(newTestCtx(&out))
		if err != errBad {
			t.Fatalf("err %v, want the handler's error itself", err)
		}
	})
}

func TestToleranceDelay(t *testing.T) {
	var out bytes.Buffer
	runs := 0
	start := time.Now()
	err := Tolerance(operation.Tolerance{Retry: true, RetryAttempts: 3, RetryDelay: 20 * time.Millisecond})(failing(2, &runs))(newTestCtx(&out))
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("3 attempts took %v, want at least two delays of 20ms", elapsed)
	}
}

// TestToleranceTimeout checks that RetryTimeout bounds the whole run, a running attempt included,
// and that the run still fails with the last attempt's error.
func TestToleranceTimeout(t *testing.T) {
	t.Run("bounds an attempt", func(t *testing.T) {
		var out bytes.Buffer
		start := time.Now()
		conf := operation.Tolerance{Retry: true, RetryAttempts: 3, RetryTimeout: 20 * time.Millisecond}
		err := Tolerance(conf)(func(ctx *operation.Context) error {
			select {
			case <-ctx.Done():
				return fmt.Errorf("slow: %w", ctx.Err())
			case <-time.After(5 * time.Second):
				return errors.New("attempt not bounded")
			}
		})(newTestCtx(&out))
		if !errors.Is(err, context.DeadlineExceeded) || err.Error() != "slow: context deadline exceeded" {
			t.Fatalf("err %v, want the attempt's own error", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("took %v, want the attempt cut at RetryTimeout", elapsed)
		}
	})

	t.Run("unlimited attempts keep the last error", func(t *testing.T) {
		var out bytes.Buffer
		runs := 0
		conf := operation.Tolerance{Retry: true, RetryDelay: 5 * time.Millisecond, RetryTimeout: 30 * time.Millisecond}
		err := Tolerance(conf)(func(*operation.Context) error {
			runs++
			return errors.New("db down")
		})(newTestCtx(&out))
		if err == nil || err.Error() != "db down" {
			t.Fatalf("err %v, want the last attempt's error, not the timeout", err)
		}
		if runs < 2 {
			t.Fatalf("ran %d times, want retries until the timeout", runs)
		}
	})
}

// TestToleranceStopsWithContext checks that the wait for the next attempt ends with the operation's
// context.
func TestToleranceStopsWithContext(t *testing.T) {
	var out bytes.Buffer
	ctx := newTestCtx(&out)
	parent, cancel := context.WithCancel(ctx.Context)
	ctx.Context = parent

	runs := 0
	start := time.Now()
	err := Tolerance(operation.Tolerance{Retry: true, RetryAttempts: 3, RetryDelay: time.Hour})(func(*operation.Context) error {
		runs++
		cancel()
		return errors.New("fail")
	})(ctx)
	if err == nil || err.Error() != "fail" || runs != 1 {
		t.Fatalf("err %v after %d runs, want the first attempt's error", err, runs)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("took %v, want the wait cut by the context", elapsed)
	}
}

// TestToleranceContext checks that every attempt starts from the same context, so that inner
// middlewares do not pile values up, and that the context is restored afterwards.
func TestToleranceContext(t *testing.T) {
	type key struct{}
	var out bytes.Buffer
	ctx := newTestCtx(&out)
	parent := ctx.Context

	var depths []int
	inner := func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			depth, _ := ctx.Context.Value(key{}).(int)
			ctx.Context = context.WithValue(ctx.Context, key{}, depth+1)
			return next(ctx)
		}
	}
	runs := 0
	handler := operation.Chain(failing(2, &runs),
		Tolerance(operation.Tolerance{Retry: true, RetryAttempts: 3, RetryTimeout: time.Minute}),
		inner,
		func(next operation.Handler) operation.Handler {
			return func(ctx *operation.Context) error {
				depths = append(depths, ctx.Context.Value(key{}).(int))
				return next(ctx)
			}
		},
	)
	if err := handler(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(depths, []int{1, 1, 1}) {
		t.Fatalf("value depth per attempt %v, want 1 each time", depths)
	}
	if ctx.Context != parent {
		t.Fatal("ctx.Context not restored")
	}
}

// TestToleranceRecordsLastAttempt checks that an outer Recorder logs the code of the attempt that ended
// the run.
func TestToleranceRecordsLastAttempt(t *testing.T) {
	var out bytes.Buffer
	attempt := 0
	handler := operation.Chain(func(*operation.Context) error {
		attempt++
		return operation.Error{Code: fmt.Sprintf("attempt%d", attempt), Message: "failed"}
	}, Recorder(nil, nil, nil), Tolerance(operation.Tolerance{Retry: true, RetryAttempts: 2}))
	_ = handler(newTestCtx(&out))

	if lines := logLines(t, &out); len(lines) != 2 || lines[1][logger.ErrorCodeLogKey] != "attempt2" {
		t.Fatalf("logged %v, want one failure with code attempt2", lines)
	}
}

func TestToleranceAllocations(t *testing.T) {
	var out bytes.Buffer
	ctx := newTestCtx(&out)
	handler := Tolerance(operation.DefaultTolerance())(func(*operation.Context) error { return nil })
	if allocs := testing.AllocsPerRun(100, func() { _ = handler(ctx) }); allocs != 0 {
		t.Fatalf("%v allocations per successful run, want 0", allocs)
	}
}
