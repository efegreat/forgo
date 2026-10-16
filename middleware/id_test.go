package middleware

import (
	"bytes"
	"strings"
	"testing"
	"time"

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
			if err != nil || id.Version() != 7 || id.Variant() != uuid.RFC4122 {
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

// New IDs carry their time: a later one sorts after an earlier one, and parses back to its millisecond.
func TestNewIDTime(t *testing.T) {
	before := time.Now()
	first := newID()
	time.Sleep(2 * time.Millisecond)
	second := newID()
	if first >= second {
		t.Fatalf("%s sorts after the later %s", first, second)
	}
	id := uuid.MustParse(first)
	sec, nsec := id.Time().UnixTime()
	if got := time.Unix(sec, nsec); got.Before(before.Truncate(time.Millisecond)) || got.After(before.Add(time.Second)) {
		t.Fatalf("%s holds %v, generated at %v", first, got, before)
	}
}

func TestNewIDAllocs(t *testing.T) {
	if allocs := testing.AllocsPerRun(100, func() { _ = newID() }); allocs != 1 {
		t.Fatalf("newID allocated %v times, want only its string", allocs)
	}
}

func BenchmarkNewID(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = newID()
	}
}
