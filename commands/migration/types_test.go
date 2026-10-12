package migration

import (
	"errors"
	"testing"
)

func TestStringsOfUnknownValues(t *testing.T) {
	if got := Direction(9).String(); got != "Direction(9)" {
		t.Fatal(got)
	}
	if got := StatusState(9).String(); got != "StatusState(9)" {
		t.Fatal(got)
	}
}

func TestExecutionErrorMessage(t *testing.T) {
	cause := errors.New("syntax error")
	for _, tc := range []struct {
		err  *ExecutionError
		want string
	}{
		{&ExecutionError{ID: "001_a.sql", Direction: Up, Statement: 2, Err: cause}, `migration: up "001_a.sql" statement 2: syntax error`},
		{&ExecutionError{ID: "001_a.sql", Direction: Down, Err: cause}, `migration: down "001_a.sql": syntax error`},
	} {
		if got := tc.err.Error(); got != tc.want || !errors.Is(tc.err, cause) {
			t.Fatalf("got %q", got)
		}
	}
}
