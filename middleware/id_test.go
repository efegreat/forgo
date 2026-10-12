package middleware

import (
	"bytes"
	"strings"
	"testing"

	"github.com/efegreat/forgo/operation"

	"github.com/google/uuid"
)

func TestID(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		keep bool
	}{
		{"empty", "", false},
		{"uuid", "0199c3a4-5b6c-7d8e-9f00-112233445566", true},
		{"visible ascii", "req_42.retry~1:A-Z!", true},
		{"max length", strings.Repeat("a", maxIDLen), true},
		{"too long", strings.Repeat("a", maxIDLen+1), false},
		{"space", "req 42", false},
		{"tab", "req\t42", false},
		{"control", "req\x0042", false},
		{"delete", "req\x7f", false},
		{"non-ascii", "réq", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			ctx := newTestCtx(&out)
			ctx.ID = tc.in

			var seen string
			err := ID()(func(ctx *operation.Context) error {
				seen = ctx.ID
				return nil
			})(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if tc.keep {
				if seen != tc.in {
					t.Fatalf("ID = %q, want the incoming %q kept", seen, tc.in)
				}
				return
			}
			id, err := uuid.Parse(seen)
			if err != nil || id.Version() != 7 {
				t.Fatalf("ID = %q, want a new UUIDv7 (err %v)", seen, err)
			}
		})
	}
}

func TestIDUnique(t *testing.T) {
	seen := make(map[string]bool)
	handler := ID()(func(ctx *operation.Context) error {
		if seen[ctx.ID] {
			t.Fatalf("ID %q generated twice", ctx.ID)
		}
		seen[ctx.ID] = true
		return nil
	})
	var out bytes.Buffer
	for range 1000 {
		if err := handler(newTestCtx(&out)); err != nil {
			t.Fatal(err)
		}
	}
}
