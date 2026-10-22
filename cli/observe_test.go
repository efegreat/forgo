package cli

import (
	"context"
	"testing"
	"time"

	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/operation"
)

// TestOperationObserver checks that the observer sees a command once, failed or not, with the error it returned.
func TestOperationObserver(t *testing.T) {
	errFailed := operation.Error{Code: "failed", Status: operation.StatusUnavailable}
	for _, tc := range []struct {
		name     string
		args     []string
		run      func(context.Context) error
		wantCode string
	}{
		{"success", []string{"app", "a"}, func(context.Context) error { return nil }, ""},
		{"error", []string{"app", "a"}, func(context.Context) error { return errFailed }, "failed"},
		{"panic", []string{"app", "a"}, func(context.Context) error { panic("boom") }, "internal"},
		{"invalid flags", []string{"app", "a", "-unknown"}, func(context.Context) error { return nil }, "command_parse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &observertest.Observer{}
			cfg := Config{Commands: []CommandConfig{{Name: "a", Command: runCommand(tc.run)}}}
			err := newTestCLI(tc.args, cfg, WithProviders(obs.Provider())).Run()

			e := obs.Last(t, 1)
			if e.Type != OperationTypeCommand || e.Name != "a" || e.Err != err || e.Error.Code != tc.wantCode {
				t.Fatalf("observed %s %q with %v (code %q), want %v (code %q)", e.Type, e.Name, e.Err, e.Error.Code, err, tc.wantCode)
			}
		})
	}
}

// A command whose lock another run holds did not run, so it is not observed, like it is not logged.
func TestOperationObserverSkipsLockedCommands(t *testing.T) {
	obs := &observertest.Observer{}
	cfg := Config{Commands: []CommandConfig{{
		Name:    "a",
		Lock:    operation.Lock{Key: "cmd", TTL: time.Minute},
		Command: runCommand(func(context.Context) error { return nil }),
	}}}
	err := newTestCLI([]string{"app", "a"}, cfg, WithProviders(obs.Provider(), func() operation.ILocker { return busyLocker{} })).Run()
	if err == nil {
		t.Fatal("the command ran")
	}
	if obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatal("observed a command whose lock was held")
	}
}

// TestLockObserver checks that a command whose lock another run holds is reported with its name.
func TestLockObserver(t *testing.T) {
	obs := &observertest.LockObserver{}
	cfg := Config{Commands: []CommandConfig{{
		Name:    "a",
		Lock:    operation.Lock{Key: "cmd", TTL: time.Minute},
		Command: runCommand(func(context.Context) error { return nil }),
	}}}
	_ = newTestCLI([]string{"app", "a"}, cfg, WithProviders(obs.Provider(), func() operation.ILocker { return busyLocker{} })).Run()

	locks := obs.Locks()
	if len(locks) != 1 || locks[0] != (observertest.Lock{Type: OperationTypeCommand, Name: "a", Key: "cmd", Held: true}) {
		t.Fatalf("reported %+v", locks)
	}
}
