package middleware

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// fakeLocker records its calls. held makes every Lock lose the race.
type fakeLocker struct {
	held      bool
	lockErr   error
	unlockErr error

	calls     []string // "lock <key> <ttl>" and "unlock <key>", in order
	unlockCtx context.Context
}

func (l *fakeLocker) Lock(_ context.Context, key string, ttl time.Duration) (bool, error) {
	l.calls = append(l.calls, "lock "+key+" "+ttl.String())
	if l.lockErr != nil {
		return false, l.lockErr
	}
	return !l.held, nil
}

func (l *fakeLocker) WaitLock(ctx context.Context, key string, _ int, _, ttl time.Duration) (bool, error) {
	return l.Lock(ctx, key, ttl)
}

func (l *fakeLocker) Unlock(ctx context.Context, key string) error {
	l.calls = append(l.calls, "unlock "+key)
	l.unlockCtx = ctx
	return l.unlockErr
}

func lockOf(lock operation.Lock) operation.LockCrafter {
	return func(*operation.Context) operation.Lock { return lock }
}

func TestLocker(t *testing.T) {
	enabled := operation.Lock{Key: "k", TTL: time.Minute}
	errBackend := errors.New("redis down")
	errUnlock := errors.New("not held")
	errConflict := errors.New("locked elsewhere")

	for _, tc := range []struct {
		name       string
		locker     *fakeLocker
		crafter    operation.LockCrafter
		onConflict func(*operation.Context) error
		wantErr    error
		wantRun    bool
		wantCalls  []string
		wantLog    string // message of the only log line, if any
	}{
		{"nil crafter", &fakeLocker{}, nil, nil, nil, true, nil, ""},
		{"disabled lock", &fakeLocker{}, lockOf(operation.Lock{Key: "k"}), nil, nil, true, nil, ""},
		{"acquired", &fakeLocker{}, lockOf(enabled), nil, nil, true, []string{"lock k 1m0s", "unlock k"}, ""},
		{"held, skipped", &fakeLocker{held: true}, lockOf(enabled), nil, nil, false, []string{"lock k 1m0s"}, "operation already running, skipping"},
		{"held, onConflict", &fakeLocker{held: true}, lockOf(enabled), func(*operation.Context) error { return errConflict }, errConflict, false, []string{"lock k 1m0s"}, "operation already running, skipping"},
		{"lock error", &fakeLocker{lockErr: errBackend}, lockOf(enabled), nil, errBackend, false, []string{"lock k 1m0s"}, "failed to acquire operation lock"},
		{"unlock error", &fakeLocker{unlockErr: errUnlock}, lockOf(enabled), nil, nil, true, []string{"lock k 1m0s", "unlock k"}, "failed to release operation lock"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			runs := 0
			err := Locker(tc.locker, tc.crafter, tc.onConflict)(ok(&runs))(newTestCtx(&out))
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("err %v, want %v", err, tc.wantErr)
			}
			if (runs == 1) != tc.wantRun {
				t.Fatalf("ran %d times, want run %v", runs, tc.wantRun)
			}
			if !slices.Equal(tc.locker.calls, tc.wantCalls) {
				t.Fatalf("locker calls %q, want %q", tc.locker.calls, tc.wantCalls)
			}

			lines := logLines(t, &out)
			if tc.wantLog == "" {
				if len(lines) != 0 {
					t.Fatalf("logged %v, want nothing", lines)
				}
				return
			}
			if len(lines) != 1 || lines[0]["message"] != tc.wantLog {
				t.Fatalf("logged %v, want one %q line", lines, tc.wantLog)
			}
			if lines[0]["level"] == "error" && lines[0][logger.ErrorSeverityLogKey] != float64(operation.SeverityCritical) {
				t.Fatalf("error line %v, want critical severity", lines[0])
			}
		})
	}
}

func TestLockerWithoutLocker(t *testing.T) {
	var out bytes.Buffer
	runs := 0

	disabled := Locker(nil, lockOf(operation.Lock{Key: "k"}), nil)(ok(&runs))
	if err := disabled(newTestCtx(&out)); err != nil || runs != 1 {
		t.Fatalf("disabled lock: err %v, runs %d", err, runs)
	}

	runs = 0
	enabled := Locker(nil, lockOf(operation.Lock{Key: "k", TTL: time.Second}), nil)(ok(&runs))
	if err := enabled(newTestCtx(&out)); !errors.Is(err, ErrNoLocker) || runs != 0 {
		t.Fatalf("enabled lock: err %v, runs %d", err, runs)
	}
}

// TestLockerReleases checks that the lock is released after a panic, and with a live context after
// the operation's context ended.
func TestLockerReleases(t *testing.T) {
	var out bytes.Buffer
	l := &fakeLocker{}
	ctx := newTestCtx(&out)
	parent, cancel := context.WithCancel(ctx.Context)
	ctx.Context = parent

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic swallowed")
			}
		}()
		_ = Locker(l, lockOf(operation.Lock{Key: "k", TTL: time.Minute}), nil)(func(*operation.Context) error {
			cancel()
			panic("boom")
		})(ctx)
	}()

	if !slices.Equal(l.calls, []string{"lock k 1m0s", "unlock k"}) {
		t.Fatalf("locker calls %q, want the lock released after the panic", l.calls)
	}
	if err := l.unlockCtx.Err(); err != nil {
		t.Fatalf("Unlock got a context that ended (%v), it would fail to release the lock", err)
	}
}
