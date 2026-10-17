package operation

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"
)

func TestStatusString(t *testing.T) {
	// Every status declared in error.go must have a name of its own.
	declared := declaredStatuses(t)
	if len(declared) != len(statusNames) {
		t.Fatalf("error.go declares %d statuses, statusNames names %d", len(declared), len(statusNames))
	}
	seen := make(map[string]string, len(statusNames))
	for i, name := range declared {
		got := Status(i).String()
		if got == "" || (i > 0 && got == "unknown") {
			t.Fatalf("%s has no name", name)
		}
		if other, ok := seen[got]; ok {
			t.Fatalf("%s and %s are both named %q", other, name, got)
		}
		seen[got] = name
	}

	for _, tc := range []struct {
		status Status
		want   string
	}{
		{StatusUnknown, "unknown"},
		{StatusNotFound, "not_found"},
		{StatusRateLimited, "rate_limited"},
		{StatusUnimplemented, "unimplemented"},
		{Status(len(statusNames)), "unknown"},
		{Status(255), "unknown"},
	} {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("Status(%d).String() = %q, want %q", tc.status, got, tc.want)
		}
	}
	if got := fmt.Sprint(StatusPreconditionFailed); got != "precondition_failed" {
		t.Errorf("fmt prints %q", got)
	}
}

// declaredStatuses returns the names of the Status constants of error.go, in order.
func declaredStatuses(t *testing.T) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "error.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST || len(gd.Specs) == 0 {
			continue
		}
		if typ, ok := gd.Specs[0].(*ast.ValueSpec).Type.(*ast.Ident); !ok || typ.Name != "Status" {
			continue
		}
		for _, spec := range gd.Specs {
			for _, name := range spec.(*ast.ValueSpec).Names {
				names = append(names, name.Name)
			}
		}
	}
	if len(names) == 0 {
		t.Fatal("no Status constants in error.go")
	}
	return names
}

func TestErrorOf(t *testing.T) {
	cause := errors.New("boom")
	notFound := errNotFound.Format("42")

	for _, tc := range []struct {
		name string
		err  error
		want Error
	}{
		{"error", notFound, notFound},
		{"wrapped", fmt.Errorf("load: %w", notFound), notFound},
		{"pointer", errors.Join(errors.New("load"), &notFound), notFound},
		{"joined", errors.Join(cause, notFound), notFound},
		{"plain", cause, NewFallbackError(cause)},
		{"nil", nil, NewFallbackError(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ErrorOf(tc.err); got != tc.want {
				t.Fatalf("ErrorOf = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestErrorOfAllocations(t *testing.T) {
	for _, err := range []error{
		errNotFound,
		fmt.Errorf("load: %w", errNotFound),
		errors.New("boom"),
	} {
		if allocs := testing.AllocsPerRun(100, func() { _ = ErrorOf(err) }); allocs != 0 {
			t.Errorf("ErrorOf(%v): %v allocations, want 0", err, allocs)
		}
	}
}

type stubLocker struct {
	acquired bool
	err      error
}

func (l stubLocker) Lock(context.Context, string, time.Duration) (bool, error) {
	return l.acquired, l.err
}
func (l stubLocker) WaitLock(context.Context, string, int, time.Duration, time.Duration) (bool, error) {
	return l.acquired, l.err
}
func (stubLocker) Unlock(context.Context, string) error { return nil }

type lockRecorder struct {
	held, failed []string
	err          error
}

func (*lockRecorder) Start(*Context)            {}
func (*lockRecorder) End(*Context, Observation) {}
func (r *lockRecorder) LockHeld(_ context.Context, key string) {
	r.held = append(r.held, key)
}
func (r *lockRecorder) LockFailed(_ context.Context, key string, err error) {
	r.failed, r.err = append(r.failed, key), err
}

type plainObserver struct{}

func (plainObserver) Start(*Context)            {}
func (plainObserver) End(*Context, Observation) {}

func TestObserveLocks(t *testing.T) {
	down := errors.New("redis down")
	for _, tc := range []struct {
		name               string
		locker             stubLocker
		wantHeld, wantFail int
	}{
		{"acquired", stubLocker{acquired: true}, 0, 0},
		{"held", stubLocker{}, 1, 0},
		{"failed", stubLocker{err: down}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &lockRecorder{}
			l := ObserveLocks(tc.locker, r)
			acquired, err := l.Lock(context.Background(), "k", time.Minute)
			if acquired != tc.locker.acquired || err != tc.locker.err {
				t.Fatalf("Lock = %v, %v, want the locker's %v, %v", acquired, err, tc.locker.acquired, tc.locker.err)
			}
			if len(r.held) != tc.wantHeld || len(r.failed) != tc.wantFail {
				t.Fatalf("held %v, failed %v", r.held, r.failed)
			}
			if tc.wantFail == 1 && (r.failed[0] != "k" || r.err != down) {
				t.Fatalf("failed %v with %v", r.failed, r.err)
			}
			// WaitLock is application code's, never the Locker middleware's.
			_, _ = l.WaitLock(context.Background(), "k", 2, 0, time.Minute)
			if len(r.held)+len(r.failed) != tc.wantHeld+tc.wantFail {
				t.Fatal("WaitLock was reported")
			}
		})
	}

	held := stubLocker{}
	if l := ObserveLocks(held, plainObserver{}); l != ILocker(held) {
		t.Fatalf("an observer without ILockObserver wrapped the locker: %T", l)
	}
	if l := ObserveLocks(nil, &lockRecorder{}); l != nil {
		t.Fatalf("a nil locker became %T", l)
	}
	if l := ObserveLocks(held, nil); l != ILocker(held) {
		t.Fatalf("a nil observer wrapped the locker: %T", l)
	}
}
