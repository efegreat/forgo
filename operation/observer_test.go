package operation

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
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
