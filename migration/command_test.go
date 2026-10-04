package migration

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

type runnerStub struct {
	action    string
	steps     int
	direction Direction
	count     int
	err       error
	statuses  []Status
	plan      []PlannedMigration
	ctx       context.Context
}

func (s *runnerStub) Up(ctx context.Context, n int) (int, error) {
	s.action, s.steps, s.ctx = "up", n, ctx
	return s.count, s.err
}
func (s *runnerStub) Down(ctx context.Context, n int) (int, error) {
	s.action, s.steps, s.ctx = "down", n, ctx
	return s.count, s.err
}
func (s *runnerStub) Status(ctx context.Context) ([]Status, error) {
	s.action, s.ctx = "status", ctx
	return s.statuses, s.err
}
func (s *runnerStub) Plan(ctx context.Context, d Direction, n int) ([]PlannedMigration, error) {
	s.action, s.direction, s.steps, s.ctx = "plan", d, n, ctx
	return s.plan, s.err
}

type creatorStub struct {
	dir, name string
	called    bool
}

func (s *creatorStub) Create(dir, name string) (string, error) {
	s.dir, s.name, s.called = dir, name, true
	return "migrations/001_create_users.sql", nil
}

func parsed(t *testing.T, args []string, r Runner, c FileCreator, output io.Writer) *Command {
	t.Helper()
	cmd := NewCommand(r, c, output)
	set := flag.NewFlagSet("migrate", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	cmd.Init(set)
	if err := set.Parse(args); err != nil {
		t.Fatal(err)
	}
	return cmd
}

func TestActionDispatchAndStepDefaults(t *testing.T) {
	for _, tc := range []struct {
		args      []string
		action    string
		steps     int
		direction Direction
	}{
		{[]string{"-a=up"}, "up", 0, Up},
		{[]string{"-action=up", "-steps=3"}, "up", 3, Up},
		{[]string{"-a=down"}, "down", 1, Up},
		{[]string{"-a=down", "-steps=5"}, "down", 5, Up},
		{[]string{"-a=status"}, "status", 0, Up},
		{[]string{"-a=plan"}, "plan", 0, Up},
		{[]string{"-a=plan", "-direction=down"}, "plan", 1, Down},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			r := &runnerStub{}
			cmd := parsed(t, tc.args, r, &creatorStub{}, io.Discard)
			if err := cmd.Validate(); err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), "trace", "original")
			if err := cmd.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if r.action != tc.action || r.steps != tc.steps || r.direction != tc.direction || r.ctx != ctx {
				t.Fatalf("unexpected dispatch: %+v", r)
			}
		})
	}
}

func TestInvalidFlagCombinations(t *testing.T) {
	for _, args := range [][]string{
		{}, {"-a=invalid"}, {"-a=up", "-steps=-1"}, {"-a=down", "-steps=0"},
		{"-a=down", "-steps=-1"}, {"-a=status", "-steps=0"},
		{"-a=up", "-direction=up"}, {"-a=down", "-name=users"},
		{"-a=plan", "-name=users"}, {"-a=plan", "-direction=sideways"},
		{"-a=plan", "-direction=down", "-steps=0"},
		{"-a=new"}, {"-a=new", "-name=users", "-steps=0"},
		{"-a=up", "-dir=./other"}, {"-a=up", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			r, c := &runnerStub{}, &creatorStub{}
			cmd := parsed(t, args, r, c, io.Discard)
			if err := cmd.Run(context.Background()); err == nil {
				t.Fatal("expected validation failure")
			}
			if r.action != "" || c.called {
				t.Fatal("invalid command executed")
			}
		})
	}
}

func TestNewDoesNotNeedMigrator(t *testing.T) {
	c := &creatorStub{}
	var output bytes.Buffer
	cmd := parsed(t, []string{"-a=new", "-name=create_users", "-dir=./custom"}, nil, c, &output)
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.dir != "./custom" || c.name != "create_users" || !strings.Contains(output.String(), "created migration:") {
		t.Fatalf("creator=%+v, output=%q", c, output.String())
	}
}

func TestPlanDisplaysStatementsInOrder(t *testing.T) {
	r := &runnerStub{plan: []PlannedMigration{
		{ID: "001_users.sql", Direction: Up, Transaction: true, Statements: []string{"CREATE TABLE users (\n  id BIGINT\n);", "CREATE INDEX idx_id ON users(id);"}},
		{ID: "002_empty.sql", Direction: Up},
	}}
	var output bytes.Buffer
	cmd := parsed(t, []string{"-a=plan"}, r, nil, &output)
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := output.String()
	for _, fragment := range []string{"Direction: UP", "Migrations: 2", "[1] 001_users.sql", "Transaction: requested", "1. CREATE TABLE", "2. CREATE INDEX", "tracking update only"} {
		if !strings.Contains(s, fragment) {
			t.Fatalf("missing %q in %s", fragment, s)
		}
	}
	if strings.Index(s, "CREATE TABLE") > strings.Index(s, "CREATE INDEX") {
		t.Fatal("statement order changed")
	}
}

func TestStatusOutput(t *testing.T) {
	at := time.Date(2026, 10, 3, 20, 0, 0, 0, time.UTC)
	r := &runnerStub{statuses: []Status{
		{ID: "001_users.sql", State: StatusApplied, AppliedAt: &at},
		{ID: "002_pending.sql", State: StatusPending},
		{ID: "003_unknown.sql", State: StatusUnknown, AppliedAt: &at},
	}}
	var output bytes.Buffer
	cmd := parsed(t, []string{"-a=status"}, r, nil, &output)
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"applied", "pending", "unknown", at.Format(time.RFC3339)} {
		if !strings.Contains(output.String(), fragment) {
			t.Fatalf("missing %q", fragment)
		}
	}
}

type brokenWriter struct{ err error }

func (w brokenWriter) Write([]byte) (int, error) { return 0, w.err }

func TestOutputAndExecutionErrorsPreserved(t *testing.T) {
	executionErr, outputErr := errors.New("migration failed"), errors.New("output failed")
	r := &runnerStub{count: 2, err: executionErr}
	cmd := parsed(t, []string{"-a=up"}, r, nil, brokenWriter{outputErr})
	err := cmd.Run(context.Background())
	if !errors.Is(err, executionErr) || !errors.Is(err, outputErr) {
		t.Fatalf("err=%v", err)
	}
	var output bytes.Buffer
	cmd = parsed(t, []string{"-a=up"}, r, nil, &output)
	if err := cmd.Run(context.Background()); !errors.Is(err, executionErr) || !strings.Contains(output.String(), "2 migrations completed") {
		t.Fatalf("output=%q, err=%v", output.String(), err)
	}
}

func TestCancelledCommandDoesNotExecute(t *testing.T) {
	r := &runnerStub{}
	cmd := parsed(t, []string{"-a=up"}, r, nil, io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cmd.Run(ctx); !errors.Is(err, context.Canceled) || r.action != "" {
		t.Fatalf("action=%q, err=%v", r.action, err)
	}
}

func TestUninitializedCommandFails(t *testing.T) {
	cmd := NewCommand(&runnerStub{}, nil, io.Discard)
	if err := cmd.Validate(); err == nil {
		t.Fatal("expected initialization error")
	}
}
