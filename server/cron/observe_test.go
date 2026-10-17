package cron

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/operation"
)

// TestOperationObserver checks that the observer sees a run once, with its retries, and a panic as the error
// of Recovery.
func TestOperationObserver(t *testing.T) {
	errFlaky := operation.Error{Code: "flaky", Status: operation.StatusUnavailable}
	tolerance := operation.Tolerance{Retry: true, RetryAttempts: 3, RetryIf: func(err error) bool {
		return errors.Is(err, errFlaky)
	}}
	for _, tc := range []struct {
		name     string
		run      func(attempt int) error
		wantRuns int
		wantCode string
	}{
		{"success", func(int) error { return nil }, 1, ""},
		{"retried", func(attempt int) error {
			if attempt < 3 {
				return errFlaky
			}
			return nil
		}, 3, ""},
		{"failed", func(int) error { return errFlaky }, 3, "flaky"},
		{"panic", func(int) error { panic("boom") }, 1, "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &observertest.Observer{}
			var attempts int
			cfg := Config{Jobs: []JobConfig{{
				Name: "job", Schedule: "@daily",
				Job: As(func() IJob {
					return retryJob{
						tolerance: tolerance,
						run: func(context.Context) error {
							attempts++
							return tc.run(attempts)
						},
					}
				}),
			}}}
			initRun(t, cfg, new(logBuffer), resolver(t, obs.Provider()))()

			if attempts != tc.wantRuns {
				t.Fatalf("%d attempts, want %d", attempts, tc.wantRuns)
			}
			e := obs.Last(t, 1)
			if e.Type != OperationTypeCronJob || e.Name != "job" || e.Error.Code != tc.wantCode {
				t.Fatalf("observed %s %q with code %q, want %q", e.Type, e.Name, e.Error.Code, tc.wantCode)
			}
		})
	}
}

type heldLocker struct{}

func (heldLocker) Lock(context.Context, string, time.Duration) (bool, error) { return false, nil }
func (l heldLocker) WaitLock(ctx context.Context, key string, _ int, _, ttl time.Duration) (bool, error) {
	return l.Lock(ctx, key, ttl)
}
func (heldLocker) Unlock(context.Context, string) error { return nil }

// A run skipped because another holds its lock did not happen, so it is not observed, like it is not logged.
func TestOperationObserverSkipsLockedRuns(t *testing.T) {
	obs := &observertest.Observer{}
	cfg := Config{Jobs: []JobConfig{{
		Name: "job", Schedule: "@daily", Job: As(newNoopJob),
		Lock: operation.Lock{Key: "job", TTL: time.Minute},
	}}}
	initRun(t, cfg, new(logBuffer), resolver(t, obs.Provider(), func() operation.ILocker { return heldLocker{} }))()

	if obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatal("observed a run whose lock was held")
	}
}

func TestOperationObserverWithoutBaseMiddlewares(t *testing.T) {
	obs := &observertest.Observer{}
	cfg := Config{
		ServerConfig: forgo.ServerConfig{DisableBaseMiddlewares: true},
		Jobs:         []JobConfig{{Name: "job", Schedule: "@daily", Job: As(newNoopJob)}},
	}
	initRun(t, cfg, new(logBuffer), resolver(t, obs.Provider()))()

	if obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatal("observed a run without base middlewares")
	}
}
