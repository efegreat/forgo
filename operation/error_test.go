package operation

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

var errNotFound = Error{Code: "not_found", Status: StatusNotFound, Message: "order %s not found", Severity: SeverityLight}

func TestErrorText(t *testing.T) {
	cause := errors.New("pq: connection refused")
	db := Error{Code: "db", Message: "database unavailable"}.Wrap(cause)

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"no cause", Error{Code: "c", Message: "failed"}, "failed"},
		{"cause", db, "database unavailable: pq: connection refused"},
		{"cause without message", Error{Code: "c"}.Wrap(cause), "pq: connection refused"},
		{"nested", Error{Code: "order", Message: "order failed"}.Wrap(db), "order failed: database unavailable: pq: connection refused"},
		{"behind a wrapper", fmt.Errorf("charge order 7: %w", db), "charge order 7: database unavailable: pq: connection refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Fatalf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestErrorDerivesCopies(t *testing.T) {
	cause := errors.New("no rows")
	derived := errNotFound.Format("42").Wrap(cause)

	if derived.Message != "order 42 not found" || derived.Unwrap() != cause {
		t.Fatalf("derived %+v, want the formatted message and the cause", derived)
	}
	if derived.Code != errNotFound.Code || derived.Status != errNotFound.Status || derived.Severity != errNotFound.Severity {
		t.Fatalf("derived %+v lost the fields of %+v", derived, errNotFound)
	}
	if errNotFound.Message != "order %s not found" || errNotFound.Unwrap() != nil {
		t.Fatalf("declared error changed to %+v", errNotFound)
	}
	if err := derived.Wrap(nil).Unwrap(); err != nil {
		t.Fatalf("Wrap(nil) kept cause %v", err)
	}
}

func TestErrorIs(t *testing.T) {
	wrapped := errNotFound.Format("42").Wrap(io.EOF)

	for _, tc := range []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"same error", errNotFound, errNotFound, true},
		{"formatted and wrapped", wrapped, errNotFound, true},
		{"behind a wrapper", fmt.Errorf("load: %w", wrapped), errNotFound, true},
		{"inside another error", Error{Code: "order"}.Wrap(wrapped), errNotFound, true},
		{"cause still matches", wrapped, io.EOF, true},
		{"other code", Error{Code: "conflict"}, errNotFound, false},
		{"plain target", wrapped, errors.New("order 42 not found"), false},
		{"no code, identical", Error{Message: "m"}, Error{Message: "m"}, true},
		{"no code, wrapped", Error{Message: "m"}.Wrap(io.EOF), Error{Message: "m"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(tc.err, tc.target); got != tc.want {
				t.Fatalf("errors.Is(%v, %v) = %v, want %v", tc.err, tc.target, got, tc.want)
			}
		})
	}
}

func TestErrorAsPointer(t *testing.T) {
	ptr := errNotFound.Wrap(io.EOF)

	for _, err := range []error{&ptr, errors.Join(errors.New("load"), &ptr)} {
		var oe Error
		if !errors.As(err, &oe) || oe.Code != errNotFound.Code || oe.Unwrap() != io.EOF {
			t.Fatalf("errors.As(%v) = %+v, want the pointed-to error", err, oe)
		}
		if got := NewErrorMapper().Map(err); got.Code != errNotFound.Code {
			t.Fatalf("Map(%v) code %q, want %q instead of the fallback", err, got.Code, errNotFound.Code)
		}
	}

	var code string
	if errNotFound.As(&code) {
		t.Fatal("As accepted a target that is not an *Error")
	}
}

func TestErrorMapperMap(t *testing.T) {
	cause := errors.New("boom")
	conflict := Error{Code: "conflict", Status: StatusConflict}
	never := func(err error) (Error, bool) {
		t.Fatalf("mapper ran for %v", err)
		return Error{}, false
	}
	unknown := func(error) (Error, bool) { return Error{}, false }
	knows := func(code string) ErrorMapperFunc {
		return func(err error) (Error, bool) { return Error{Code: code}.Wrap(err), true }
	}

	for _, tc := range []struct {
		name     string
		mappers  []ErrorMapperFunc
		err      error
		wantCode string
		wantErr  error
	}{
		{"error unchanged", []ErrorMapperFunc{never}, conflict.Wrap(cause), "conflict", cause},
		{"error behind a wrapper", []ErrorMapperFunc{never}, fmt.Errorf("save: %w", conflict), "conflict", nil},
		{"first mapper that knows", []ErrorMapperFunc{unknown, knows("first"), knows("second")}, cause, "first", cause},
		{"fallback", []ErrorMapperFunc{unknown}, cause, "internal", cause},
		{"no mappers", nil, cause, "internal", cause},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := NewErrorMapper(tc.mappers...).Map(tc.err)
			if got.Code != tc.wantCode || got.Unwrap() != tc.wantErr {
				t.Fatalf("Map = %+v (cause %v), want code %q and cause %v", got, got.Unwrap(), tc.wantCode, tc.wantErr)
			}
		})
	}
}

func TestNewFallbackError(t *testing.T) {
	cause := errors.New("boom")
	got := NewFallbackError(cause)
	want := Error{Code: "internal", Status: StatusUnknown, Message: "an error occurred", Severity: SeverityCritical, err: cause}
	if got != want {
		t.Fatalf("NewFallbackError = %+v, want %+v", got, want)
	}
}

func TestNewErrorMapperCopiesMappers(t *testing.T) {
	mappers := []ErrorMapperFunc{func(err error) (Error, bool) { return Error{Code: "kept"}, true }}
	m := NewErrorMapper(mappers...)
	mappers[0] = func(err error) (Error, bool) { return Error{Code: "replaced"}, true }

	if got := m.Map(errors.New("boom")); got.Code != "kept" {
		t.Fatalf("code %q, want the mapper given to NewErrorMapper", got.Code)
	}
}

func TestErrorMapperRunsEachMapperOnce(t *testing.T) {
	calls := 0
	m := NewErrorMapper(func(error) (Error, bool) {
		calls++
		return Error{}, false
	})
	oe := m.Map(errors.New("boom"))
	if calls != 1 {
		t.Fatalf("mapper called %d times, want 1", calls)
	}
	if oe.Code != "internal" {
		t.Fatalf("code %q, want fallback", oe.Code)
	}
}
