package middleware

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/operation"
)

func TestObserve(t *testing.T) {
	denied := operation.Error{Code: "denied", Status: operation.StatusPermissionDenied, Message: "denied"}
	plain := errors.New("boom")

	for _, tc := range []struct {
		name      string
		err       error
		wantError operation.Error
	}{
		{"success", nil, operation.Error{}},
		{"operation error", denied, denied},
		{"wrapped operation error", fmt.Errorf("load: %w", denied), denied},
		{"plain error", plain, operation.NewFallbackError(plain)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			obs := &observertest.Observer{}
			handler := Observe(obs)(func(*operation.Context) error {
				if obs.Starts() != 1 {
					t.Error("the operation ran before Start")
				}
				time.Sleep(time.Millisecond)
				return tc.err
			})

			if err := handler(newTestCtx(&out)); err != tc.err {
				t.Fatalf("returned %v, want %v", err, tc.err)
			}
			e := obs.Last(t, 1)
			if e.Type != "test" || e.Name != "op" {
				t.Fatalf("observed %s %q", e.Type, e.Name)
			}
			if e.Err != tc.err || e.Error != tc.wantError {
				t.Fatalf("observed %v and %+v, want %v and %+v", e.Err, e.Error, tc.err, tc.wantError)
			}
			if e.Elapsed < time.Millisecond {
				t.Fatalf("elapsed %v, want at least the operation's time", e.Elapsed)
			}
		})
	}
}

func TestObserveWithoutObserver(t *testing.T) {
	var runs int
	handler := ok(&runs)
	// The chain is the same without an observer.
	if got := Observe(nil)(handler); fmt.Sprintf("%p", got) != fmt.Sprintf("%p", handler) {
		t.Fatal("Observe(nil) wrapped the handler")
	}
}

func TestObserveRecoveredPanic(t *testing.T) {
	var out bytes.Buffer
	obs := &observertest.Observer{}
	handler := operation.Chain(func(*operation.Context) error { panic("boom") }, Observe(obs), Recovery(nil))

	err := handler(newTestCtx(&out))
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("returned %v, want a *PanicError", err)
	}
	if e := obs.Last(t, 1); e.Err != err || e.Error.Code != operation.NewFallbackError(nil).Code {
		t.Fatalf("observed %v (%+v), want the *PanicError", e.Err, e.Error)
	}
}

func TestObservePanicSkipsEnd(t *testing.T) {
	var out bytes.Buffer
	obs := &observertest.Observer{}
	handler := Observe(obs)(func(*operation.Context) error { panic("boom") })

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the panic did not reach the caller")
			}
		}()
		_ = handler(newTestCtx(&out))
	}()
	if obs.Starts() != 1 || len(obs.Ends()) != 0 {
		t.Fatalf("%d starts and %d ends, want only the start", obs.Starts(), len(obs.Ends()))
	}
}

func TestObserveAllocations(t *testing.T) {
	var out bytes.Buffer
	ctx := newTestCtx(&out)
	denied := operation.Error{Code: "denied", Status: operation.StatusPermissionDenied}
	for _, err := range []error{nil, denied} {
		handler := Observe(nopObserver{})(func(*operation.Context) error { return err })
		if allocs := testing.AllocsPerRun(100, func() { _ = handler(ctx) }); allocs != 0 {
			t.Errorf("%v allocations per operation that returned %v, want 0", allocs, err)
		}
	}
}

type nopObserver struct{}

func (nopObserver) Start(*operation.Context)                      {}
func (nopObserver) End(*operation.Context, operation.Observation) {}
