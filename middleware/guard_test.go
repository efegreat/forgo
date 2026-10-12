package middleware

import (
	"bytes"
	"errors"
	"testing"

	"github.com/efegreat/forgo/operation"
)

func TestGuard(t *testing.T) {
	checkErr := errors.New("check failed")
	rejectErr := errors.New("rejected by hook")
	allow := func(*operation.Context) (bool, error) { return true, nil }
	deny := func(*operation.Context) (bool, error) { return false, nil }

	for _, tc := range []struct {
		name    string
		check   func(*operation.Context) (bool, error)
		reject  func(*operation.Context) error
		wantErr error
		wantRun bool
	}{
		{"nil check allows", nil, nil, nil, true},
		{"allowed", allow, nil, nil, true},
		{"check error", func(*operation.Context) (bool, error) { return false, checkErr }, nil, checkErr, false},
		{"denied with reject", deny, func(*operation.Context) error { return rejectErr }, rejectErr, false},
		{"denied, reject returns nil", deny, func(*operation.Context) error { return nil }, nil, false},
		{"denied without reject", deny, nil, ErrRejected, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			runs := 0
			err := Guard(tc.check, tc.reject)(ok(&runs))(newTestCtx(&out))
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("err %v, want %v", err, tc.wantErr)
			}
			if (runs == 1) != tc.wantRun {
				t.Fatalf("ran %d times, want run %v", runs, tc.wantRun)
			}
		})
	}
}

func TestErrRejected(t *testing.T) {
	if ErrRejected.Status != operation.StatusPermissionDenied || ErrRejected.Severity != operation.SeverityLight {
		t.Fatalf("ErrRejected = %+v, want a light permission-denied error", ErrRejected)
	}
}
