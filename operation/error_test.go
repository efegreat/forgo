package operation

import (
	"errors"
	"testing"
)

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
	if oe.Code != "x" {
		t.Fatalf("code %q, want fallback", oe.Code)
	}
}
