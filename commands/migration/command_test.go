package migration

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/cli"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/rs/zerolog"

	sqlmigrate "github.com/rubenv/sql-migrate"
)

type traceKey struct{}

type runnerStub struct {
	action    string
	steps     int
	direction Direction
	count     int
	err       error
	statuses  []Status
	plan      []PlannedMigration
	ctx       context.Context
	// migrate runs inside Up and Down, before they return.
	migrate func(ctx context.Context)
}

func (s *runnerStub) Up(ctx context.Context, n int) (int, error) {
	s.action, s.steps, s.ctx = "up", n, ctx
	if s.migrate != nil {
		s.migrate(ctx)
	}
	return s.count, s.err
}
func (s *runnerStub) Down(ctx context.Context, n int) (int, error) {
	s.action, s.steps, s.ctx = "down", n, ctx
	if s.migrate != nil {
		s.migrate(ctx)
	}
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
	err       error
}

func (s *creatorStub) Create(dir, name string) (string, error) {
	s.dir, s.name, s.called = dir, name, true
	return "migrations/001_create_users.sql", s.err
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
			ctx := context.WithValue(context.Background(), traceKey{}, "original")
			if err := cmd.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if r.action != tc.action || r.steps != tc.steps || r.direction != tc.direction || r.ctx.Value(traceKey{}) != "original" {
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
			err := cmd.Run(context.Background())
			if err == nil || !strings.HasPrefix(err.Error(), "migration: ") {
				t.Fatalf("expected a migration validation error, got %v", err)
			}
			if r.action != "" || c.called {
				t.Fatal("invalid command executed")
			}
		})
	}
}

func TestMissingDependenciesRejected(t *testing.T) {
	if err := parsed(t, []string{"-a=up"}, nil, &creatorStub{}, io.Discard).Validate(); err == nil {
		t.Fatal("up accepted without a migrator")
	}
	if err := parsed(t, []string{"-a=new", "-name=users"}, &runnerStub{}, nil, io.Discard).Validate(); err == nil {
		t.Fatal("new accepted without a creator")
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

func TestNewReportsCreatorError(t *testing.T) {
	failed := errors.New("disk full")
	var output bytes.Buffer
	cmd := parsed(t, []string{"-a=new", "-name=create_users"}, nil, &creatorStub{err: failed}, &output)
	if err := cmd.Run(context.Background()); !errors.Is(err, failed) || output.Len() != 0 {
		t.Fatalf("output=%q, err=%v", output.String(), err)
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

func TestPlanDownOutput(t *testing.T) {
	r := &runnerStub{plan: []PlannedMigration{{ID: "002_b.sql", Direction: Down, Statements: []string{"DROP TABLE b;"}}}}
	var output bytes.Buffer
	cmd := parsed(t, []string{"-a=plan", "-direction=down"}, r, nil, &output)
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"Direction: DOWN", "Migrations: 1", "Transaction: no", "1. DROP TABLE b;"} {
		if !strings.Contains(output.String(), fragment) {
			t.Fatalf("missing %q in %s", fragment, output.String())
		}
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

func TestStatusWithoutMigrations(t *testing.T) {
	var output bytes.Buffer
	cmd := parsed(t, []string{"-a=status"}, &runnerStub{}, nil, &output)
	if err := cmd.Run(context.Background()); err != nil || output.String() != "no migrations\n" {
		t.Fatalf("output=%q, err=%v", output.String(), err)
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
	if err := cmd.Run(context.Background()); !errors.Is(err, executionErr) || !strings.Contains(output.String(), "up: 2 migrations completed") {
		t.Fatalf("output=%q, err=%v", output.String(), err)
	}
}

// report replays progress events the way Migrator does.
func report(lockWait bool, migrations ...PlannedMigration) func(context.Context) {
	return func(ctx context.Context) {
		p := progressFrom(ctx)
		if lockWait {
			p.LockWait()
		}
		for _, m := range migrations {
			p.Start(m)
			p.Done(m, 1204*time.Millisecond)
		}
	}
}

func TestMigrateReportsProgress(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-a=up"}, "waiting for the migration lock, held by another run\napplying 001_a.sql\napplied 001_a.sql (1.204s)\nup: 1 migration completed\n"},
		{[]string{"-a=down"}, "waiting for the migration lock, held by another run\nrolling back 001_a.sql\nrolled back 001_a.sql (1.204s)\ndown: 1 migration completed\n"},
	} {
		t.Run(tc.args[0], func(t *testing.T) {
			r := &runnerStub{count: 1, migrate: report(true, PlannedMigration{ID: "001_a.sql"})}
			var output bytes.Buffer
			if err := parsed(t, tc.args, r, nil, &output).Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if output.String() != tc.want {
				t.Fatalf("output:\n%s\nwant:\n%s", output.String(), tc.want)
			}
		})
	}
}

// signalWriter closes seen once a write contains want.
type signalWriter struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	want string
	seen chan struct{}
	once sync.Once
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if strings.Contains(string(p), w.want) {
		w.once.Do(func() { close(w.seen) })
	}
	return w.buf.Write(p)
}

func TestMigrateAnnouncesStop(t *testing.T) {
	out := &signalWriter{want: "stop requested", seen: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &runnerStub{migrate: func(context.Context) {
		cancel()
		select {
		case <-out.seen:
		case <-time.After(5 * time.Second):
			t.Error("no stop notice")
		}
	}}
	if err := parsed(t, []string{"-a=up"}, r, nil, out).Run(ctx); err != nil {
		t.Fatal(err)
	}
	out.mu.Lock()
	defer out.mu.Unlock()
	if !strings.HasPrefix(out.buf.String(), "stop requested: stopping after the running migration\n") {
		t.Fatalf("output=%q", out.buf.String())
	}
}

func TestCompletedRunPrintsNoStopNotice(t *testing.T) {
	var output bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	if err := parsed(t, []string{"-a=up"}, &runnerStub{}, nil, &output).Run(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	time.Sleep(10 * time.Millisecond)
	if strings.Contains(output.String(), "stop requested") {
		t.Fatalf("output=%q", output.String())
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

func TestNilOutputMeansStdout(t *testing.T) {
	if cmd := NewCommand(&runnerStub{}, nil, nil); cmd.output != os.Stdout {
		t.Fatalf("output=%v", cmd.output)
	}
}

type dbResource struct {
	name string
	db   *sql.DB
}

func (r dbResource) Name() string { return r.name }
func (r dbResource) Value() any   { return r.db }
func (r dbResource) Close() error { return nil }

func (r dbResource) New() (forgo.Resource, error) { return r, nil }

// runCLI runs the migrate command built by Factory(cfg) through the CLI.
func runCLI(t *testing.T, cfg CommandConfig, dbs []dbResource, args ...string) error {
	t.Helper()
	resources := make([]forgo.ResourceFactory, len(dbs))
	for i, db := range dbs {
		resources[i] = db
	}
	return cli.New(
		cli.WithArgs(append([]string{"app", "migrate"}, args...)),
		cli.WithLogger(logger.Config{Level: zerolog.Disabled}),
		cli.WithResources(resources...),
		cli.WithConfig(cli.Config{Commands: []cli.CommandConfig{{Name: "migrate", Command: Factory(cfg)}}}),
	).Run()
}

func mockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

const fsMigration = "-- +migrate Up\nCREATE TABLE a (id INT);\n-- +migrate Down\nDROP TABLE a;\n"

func TestFactoryRunsThroughCLI(t *testing.T) {
	db, mock := mockDB(t)
	expectRecords(mock, "postgres", false)
	var output bytes.Buffer
	cfg := CommandConfig{
		Config: Config{Dialect: "postgres"},
		FS:     fstest.MapFS{"db/migrations/001_a.sql": {Data: []byte(fsMigration)}},
		Dir:    "./db/migrations",
		Output: &output,
	}
	if err := runCLI(t, cfg, []dbResource{{"main", db}}, "-a=plan"); err != nil {
		t.Fatal(err)
	}
	if s := output.String(); !strings.Contains(s, "[1] 001_a.sql") || !strings.Contains(s, "CREATE TABLE a (id INT);") {
		t.Fatalf("output=%q", s)
	}
	assertExpectations(t, mock)
}

func TestFactoryResolvesNamedDatabase(t *testing.T) {
	primary, mock := mockDB(t)
	replica, _ := mockDB(t)
	dbs := []dbResource{{"primary", primary}, {"replica", replica}}
	cfg := CommandConfig{Config: Config{Dialect: "postgres"}, FS: fstest.MapFS{}, Output: io.Discard}
	if err := runCLI(t, cfg, dbs, "-a=status"); !errors.Is(err, forgo.ErrAmbiguousDependency) {
		t.Fatalf("expected ambiguous database, got %v", err)
	}
	cfg.Dir = "."
	cfg.Database = "primary"
	expectRecords(mock, "postgres", false)
	if err := runCLI(t, cfg, dbs, "-a=status"); err != nil {
		t.Fatal(err)
	}
	assertExpectations(t, mock)
}

func TestFactoryErrors(t *testing.T) {
	db, _ := mockDB(t)
	for name, tc := range map[string]struct {
		cfg  CommandConfig
		want error
	}{
		// sqlmock's driver is not one DetectDialect knows.
		"undetectable dialect": {CommandConfig{}, ErrUnsupportedDialect},
		"invalid FS directory": {CommandConfig{Config: Config{Dialect: "postgres"}, FS: fstest.MapFS{}, Dir: "../outside"}, nil},
		"missing database":     {CommandConfig{Config: Config{Dialect: "postgres"}, Database: "other"}, forgo.ErrResourceNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			err := runCLI(t, tc.cfg, []dbResource{{"main", db}}, "-a=status")
			if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("err=%v", err)
			}
			if !strings.HasPrefix(err.Error(), "migration: ") || strings.Contains(err.Error(), "\n") {
				t.Fatalf("want a one-line migration error, got %q", err)
			}
		})
	}
}

func TestFactoryCommandErrorsCarryCLICodes(t *testing.T) {
	db, _ := mockDB(t)
	cfg := CommandConfig{Config: Config{Dialect: "postgres"}, Output: io.Discard}
	for _, tc := range []struct {
		args []string
		code string
	}{
		{[]string{"-unknown"}, "command_parse"},
		{[]string{"-a=sideways"}, "command_validate"},
	} {
		var opErr operation.Error
		if err := runCLI(t, cfg, []dbResource{{"main", db}}, tc.args...); !errors.As(err, &opErr) || opErr.Code != tc.code {
			t.Fatalf("%v: err=%v", tc.args, err)
		}
	}
}

func TestFactoryDefaults(t *testing.T) {
	cfg := CommandConfig{}.withDefaults()
	if cfg.Dir != "migrations" || cfg.Output != os.Stdout {
		t.Fatalf("defaults: %+v", cfg)
	}
	source, err := cfg.source()
	if fileSource, ok := source.(sqlmigrate.FileMigrationSource); err != nil || !ok || fileSource.Dir != "migrations" {
		t.Fatalf("source=%#v, err=%v", source, err)
	}
	if NewCreator("").dir != "migrations" {
		t.Fatal("creator default directory differs from the command's")
	}
}
