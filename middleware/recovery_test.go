package middleware

import (
	"bytes"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/efegreat/forgo/operation"
)

var errSentinel = errors.New("sentinel")

func TestRecovery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		panic   func()
		wantMsg string
		check   func(*testing.T, error)
	}{
		{"string", func() { panic("boom") }, "panic: boom\n\n", nil},
		{"int", func() { panic(42) }, "panic: 42\n\n", nil},
		{
			"error", func() { panic(errSentinel) }, "panic: sentinel\n\n",
			func(t *testing.T, err error) {
				if !errors.Is(err, errSentinel) {
					t.Fatal("errors.Is does not see the panicked error")
				}
			},
		},
		{
			"runtime error", func() {
				var m map[string]int
				//lint:ignore SA5000 the test panics on purpose.
				m["a"] = 1
			}, "panic: assignment to entry in nil map\n\n",
			func(t *testing.T, err error) {
				var re runtime.Error
				if !errors.As(err, &re) {
					t.Fatal("errors.As does not see the runtime.Error")
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := Recovery(nil)(func(*operation.Context) error {
				tc.panic()
				return nil
			})(newTestCtx(&out))

			var p *PanicError
			if !errors.As(err, &p) {
				t.Fatalf("err %T %v, want a *PanicError", err, err)
			}
			if !strings.HasPrefix(err.Error(), tc.wantMsg) {
				t.Fatalf("message %q, want prefix %q", err.Error(), tc.wantMsg)
			}
			// The stack is taken where the panic happened, so it names the panicking function.
			if !strings.Contains(string(p.Stack), "TestRecovery") || !strings.Contains(err.Error(), "TestRecovery") {
				t.Fatalf("stack does not reach the panic site:\n%s", p.Stack)
			}
			if tc.check != nil {
				tc.check(t, err)
			}
		})
	}
}

func TestRecoveryHook(t *testing.T) {
	var out bytes.Buffer
	answered := errors.New("answered")
	var got *PanicError
	err := Recovery(func(_ *operation.Context, p *PanicError) error {
		got = p
		return answered
	})(func(*operation.Context) error { panic("boom") })(newTestCtx(&out))

	if err != answered {
		t.Fatalf("err %v, want the hook's error", err)
	}
	if got == nil || got.Value != "boom" || len(got.Stack) == 0 {
		t.Fatalf("hook got %+v, want the panic value and a stack", got)
	}
}

func TestRecoveryWithoutPanic(t *testing.T) {
	var out bytes.Buffer
	called := false
	hook := func(*operation.Context, *PanicError) error { called = true; return nil }

	err := Recovery(hook)(func(*operation.Context) error { return errSentinel })(newTestCtx(&out))
	if err != errSentinel || called {
		t.Fatalf("err %v, hook called %v, want the handler's error and no hook", err, called)
	}
}

func TestPanicErrorUnwrap(t *testing.T) {
	if err := (&PanicError{Value: "boom"}).Unwrap(); err != nil {
		t.Fatalf("Unwrap of a non-error value = %v, want nil", err)
	}
	if err := (&PanicError{Value: errSentinel}).Unwrap(); err != errSentinel {
		t.Fatalf("Unwrap = %v, want the panicked error", err)
	}
}
