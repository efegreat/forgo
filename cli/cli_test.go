package cli

import (
	"context"
	"errors"
	"flag"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/process"

	"github.com/rs/zerolog"
)

type nameCommand struct{ name string }

func (c *nameCommand) Init(fs *flag.FlagSet) { fs.StringVar(&c.name, "name", "", "") }
func (c *nameCommand) Validate() error {
	if c.name == "" {
		return errors.New("-name is required")
	}
	return nil
}
func (c *nameCommand) Run(context.Context) error { return nil }

func newNameCommand() CommandFactory { return func() Command { return &nameCommand{} } }

// funcCommand runs fn and accepts any flags.
type funcCommand struct{ fn func(context.Context) error }

func (c funcCommand) Init(*flag.FlagSet)            {}
func (c funcCommand) Validate() error               { return nil }
func (c funcCommand) Run(ctx context.Context) error { return c.fn(ctx) }

func runCommand(fn func(context.Context) error) Factory {
	return As(func() CommandFactory { return func() Command { return funcCommand{fn: fn} } })
}

type busyLocker struct{}

func (busyLocker) Lock(context.Context, string, time.Duration) (bool, error) { return false, nil }
func (busyLocker) WaitLock(context.Context, string, int, time.Duration, time.Duration) (bool, error) {
	return false, nil
}
func (busyLocker) Unlock(context.Context, string) error { return nil }

// testResource reports its Close through onClose and returns closeErr.
type testResource struct {
	onClose  func()
	closeErr error
}

func (r *testResource) Name() string { return "test" }
func (r *testResource) Value() any   { return r }
func (r *testResource) Close() error {
	if r.onClose != nil {
		r.onClose()
	}
	return r.closeErr
}

type resourceFactory struct {
	res *testResource
	err error
}

func (f resourceFactory) New() (forgo.Resource, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
}

// newTestCLI returns a CLI that runs args and logs nothing.
func newTestCLI(args []string, cfg Config, opts ...Option) *CLI {
	return New(append([]Option{
		WithArgs(args),
		WithConfig(cfg),
		WithLogger(logger.Config{Level: zerolog.Disabled}),
	}, opts...)...)
}

func TestConfigValidate(t *testing.T) {
	cmd := runCommand(func(context.Context) error { return nil })
	for _, tc := range []struct {
		name     string
		commands []CommandConfig
		want     string
	}{
		{"valid", []CommandConfig{{Name: "a", Command: cmd}, {Name: "b", Command: cmd}}, ""},
		{"no commands", nil, "no commands configured"},
		{"empty name", []CommandConfig{{Name: "a", Command: cmd}, {Command: cmd}}, "command 1 has no name"},
		{"nil command", []CommandConfig{{Name: "a"}}, `command "a" has no Command`},
		{"duplicate", []CommandConfig{{Name: "a", Command: cmd}, {Name: "a", Command: cmd}}, `duplicate command "a"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Config{Commands: tc.commands}.validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), "cli: ") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want cli: ... %s", err, tc.want)
			}
		})
	}
}

func TestConfigLookup(t *testing.T) {
	cmd := runCommand(func(context.Context) error { return nil })
	cfg := Config{Commands: []CommandConfig{{Name: "b", Command: cmd}, {Name: "c", Command: cmd}, {Name: "a", Command: cmd}}}

	found, err := cfg.lookup([]string{"app", "c", "-x"})
	if err != nil || found.Name != "c" {
		t.Fatalf("lookup c: %q, %v", found.Name, err)
	}

	for _, tc := range []struct {
		args    []string
		target  error
		message string
	}{
		{nil, ErrNoCommand, "cli: no command specified, available: b, c, a"},
		{[]string{"app"}, ErrNoCommand, "cli: no command specified, available: b, c, a"},
		{[]string{"app", "d"}, ErrUnknownCommand, `cli: unknown command "d", available: b, c, a`},
	} {
		_, err := cfg.lookup(tc.args)
		if !errors.Is(err, tc.target) || err.Error() != tc.message {
			t.Errorf("args %v: error %q, want %q", tc.args, err, tc.message)
		}
	}
}

func TestResolveLockRequiresLocker(t *testing.T) {
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer boot.Close()

	cmd := CommandConfig{Name: "locked", Lock: operation.Lock{Key: "cmd", TTL: time.Minute}, Command: As(newNameCommand)}
	if _, err := New().resolve(cmd, r); err == nil || !strings.Contains(err.Error(), "operation.ILocker") {
		t.Fatalf("error %v, want missing locker", err)
	}

	// locks are not applied without base middlewares
	c := New(WithConfig(Config{DisableBaseMiddlewares: true}))
	if _, err := c.resolve(cmd, r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCommandErrorsHaveMessages(t *testing.T) {
	c := New()
	cmd := resolvedCommand{Name: "greet", Command: newNameCommand()}
	run := c.createHandler(context.Background(), cmd)

	for _, tc := range []struct {
		args []string
		code string
		want string
	}{
		{[]string{"app", "greet", "-unknown"}, "command_parse", "invalid arguments: "},
		{[]string{"app", "greet"}, "command_validate", "invalid command: -name is required"},
	} {
		err := run(tc.args)
		var opErr operation.Error
		if !errors.As(err, &opErr) || opErr.Code != tc.code || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("args %v: error %q, want code %s and %q", tc.args, err, tc.code, tc.want)
		}
	}
}

func TestHelpIsNotAFailure(t *testing.T) {
	c := New()
	run := c.createHandler(context.Background(), resolvedCommand{Name: "greet", Command: newNameCommand()})
	for _, arg := range []string{"-h", "-help"} {
		if err := run([]string{"app", "greet", arg}); err != nil {
			t.Errorf("%s: unexpected error: %v", arg, err)
		}
	}
}

func TestEveryRunGetsAFreshCommand(t *testing.T) {
	created := 0
	factory := func() Command { created++; return &nameCommand{} }
	run := New().createHandler(context.Background(), resolvedCommand{Name: "greet", Command: factory})

	if err := run([]string{"app", "greet", "-name=x"}); err != nil {
		t.Fatal(err)
	}
	// a reused Command would still hold -name from the first run
	if err := run([]string{"app", "greet"}); err == nil {
		t.Fatal("second run passed validation with the first run's flags")
	}
	if created != 2 {
		t.Fatalf("created %d commands, want 2", created)
	}
}

func TestRunMiddlewareOrder(t *testing.T) {
	var calls []string
	record := func(name string) forgo.MiddlewareFactory {
		return forgo.MiddlewareAs(func() operation.Middleware {
			return func(next operation.Handler) operation.Handler {
				return func(ctx *operation.Context) error {
					if ctx.ID == "" {
						t.Errorf("%s middleware ran before the base chain set the operation ID", name)
					}
					calls = append(calls, name)
					return next(ctx)
				}
			}
		})
	}
	cfg := Config{
		Middlewares: []forgo.MiddlewareFactory{record("cli")},
		Commands: []CommandConfig{{
			Name:        "run",
			Middlewares: []forgo.MiddlewareFactory{record("command")},
			Command: runCommand(func(ctx context.Context) error {
				calls = append(calls, "handler")
				if op := ctx.(*operation.Context); op.Type != OperationTypeCommand || op.Name != "run" {
					t.Errorf("operation %s/%s, want %s/run", op.Type, op.Name, OperationTypeCommand)
				}
				return nil
			}),
		}},
	}
	if err := newTestCLI([]string{"app", "run"}, cfg).Run(); err != nil {
		t.Fatal(err)
	}
	if want := []string{"cli", "command", "handler"}; !slices.Equal(calls, want) {
		t.Fatalf("calls %v, want %v", calls, want)
	}
}

func TestRunFailsBeforeBootstrap(t *testing.T) {
	cmd := runCommand(func(context.Context) error { return nil })
	for _, tc := range []struct {
		name   string
		args   []string
		cfg    Config
		target error
	}{
		{"invalid config", []string{"app", "a"}, Config{Commands: []CommandConfig{{Name: "a"}}}, nil},
		{"no command", []string{"app"}, Config{Commands: []CommandConfig{{Name: "a", Command: cmd}}}, ErrNoCommand},
		{"unknown command", []string{"app", "b"}, Config{Commands: []CommandConfig{{Name: "a", Command: cmd}}}, ErrUnknownCommand},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// the resource fails with "bootstrapped" if the bootstrap is built
			res := resourceFactory{err: errors.New("bootstrapped")}
			err := newTestCLI(tc.args, tc.cfg, WithResources(res)).Run()
			if err == nil || (tc.target != nil && !errors.Is(err, tc.target)) {
				t.Fatalf("error %v, want %v", err, tc.target)
			}
			if strings.Contains(err.Error(), "bootstrapped") {
				t.Fatal("bootstrap ran")
			}
		})
	}
}

func TestRunBootstrapFailure(t *testing.T) {
	closed := false
	ran := false
	cfg := Config{Commands: []CommandConfig{{Name: "a", Command: runCommand(func(context.Context) error { ran = true; return nil })}}}
	err := newTestCLI([]string{"app", "a"}, cfg, WithResources(
		resourceFactory{res: &testResource{onClose: func() { closed = true }}},
		resourceFactory{err: errors.New("connection refused")},
	)).Run()
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("error %v, want the resource error", err)
	}
	if ran || !closed {
		t.Fatalf("ran %v, closed %v: want the command skipped and the first resource closed", ran, closed)
	}
}

func TestRunConstructorError(t *testing.T) {
	cfg := Config{Commands: []CommandConfig{{Name: "a", Command: As(func() (CommandFactory, error) {
		return nil, errors.New("no search index")
	})}}}
	if err := newTestCLI([]string{"app", "a"}, cfg).Run(); err == nil || !strings.Contains(err.Error(), "no search index") {
		t.Fatalf("error %v, want the constructor error", err)
	}
}

func TestRunClosesResourcesAfterGoroutines(t *testing.T) {
	var finished atomic.Bool
	finishedAtClose := false
	res := &testResource{
		onClose:  func() { finishedAtClose = finished.Load() },
		closeErr: errors.New("flush failed"),
	}
	cfg := Config{Commands: []CommandConfig{{Name: "a", Command: runCommand(func(context.Context) error {
		process.Go(func() {
			time.Sleep(20 * time.Millisecond)
			finished.Store(true)
		})
		return nil
	})}}}

	err := newTestCLI([]string{"app", "a"}, cfg, WithResources(resourceFactory{res: res})).Run()
	if !finishedAtClose {
		t.Fatal("resources were closed before process.Go goroutines finished")
	}
	if err == nil || !strings.Contains(err.Error(), "flush failed") {
		t.Fatalf("error %v, want the close error", err)
	}
}

func TestRunLockConflict(t *testing.T) {
	ran := false
	cfg := Config{Commands: []CommandConfig{{
		Name:    "a",
		Lock:    operation.Lock{Key: "cmd", TTL: time.Minute},
		Command: runCommand(func(context.Context) error { ran = true; return nil }),
	}}}
	err := newTestCLI([]string{"app", "a"}, cfg, WithProviders(func() operation.ILocker { return busyLocker{} })).Run()

	var opErr operation.Error
	if !errors.As(err, &opErr) || opErr.Code != "command_locked" || opErr.Status != operation.StatusAborted {
		t.Fatalf("error %v, want command_locked", err)
	}
	if ran {
		t.Fatal("command ran without its lock")
	}
}

func TestRunRecoversPanic(t *testing.T) {
	cfg := Config{Commands: []CommandConfig{{Name: "a", Command: runCommand(func(context.Context) error { panic("boom") })}}}
	if err := newTestCLI([]string{"app", "a"}, cfg).Run(); err == nil || !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("error %v, want the recovered panic", err)
	}
}

func TestRunCancelsOnSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Process.Signal cannot send os.Interrupt on windows")
	}
	cfg := Config{Commands: []CommandConfig{{Name: "a", Command: runCommand(func(ctx context.Context) error {
		p, err := os.FindProcess(os.Getpid())
		if err != nil {
			return err
		}
		if err := p.Signal(os.Interrupt); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("context was not cancelled")
		}
	})}}}
	if err := newTestCLI([]string{"app", "a"}, cfg).Run(); !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want context.Canceled", err)
	}
}

// TestRunLogsToStderr checks that logs stay out of stdout, where commands print their results, and that
// code logging without the operation's context goes through the CLI's logger while Run runs.
func TestRunLogsToStderr(t *testing.T) {
	stdout := swapFile(t, &os.Stdout)
	stderr := swapFile(t, &os.Stderr)
	before := logger.FromCtx(context.Background())

	cfg := Config{Commands: []CommandConfig{{Name: "a", Command: runCommand(func(context.Context) error {
		logger.FromCtx(context.Background()).Info().Msg("without context")
		_, err := os.Stdout.WriteString("result\n")
		return err
	})}}}
	if err := New(WithArgs([]string{"app", "a"}), WithConfig(cfg)).Run(); err != nil {
		t.Fatal(err)
	}

	if got := readFile(t, stdout); got != "result\n" {
		t.Errorf("stdout = %q, want only the command's output", got)
	}
	logs := readFile(t, stderr)
	for _, want := range []string{`"message":"operation completed"`, `"app_name":"cli","time"`, `"message":"without context"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("stderr misses %s: %q", want, logs)
		}
	}
	if logger.FromCtx(context.Background()) != before {
		t.Error("Run did not restore the default logger")
	}
}

// swapFile points *f to a temporary file until the test ends, and returns that file.
func swapFile(t *testing.T, f **os.File) *os.File {
	t.Helper()
	tmp, err := os.CreateTemp(t.TempDir(), "out")
	if err != nil {
		t.Fatal(err)
	}
	original := *f
	*f = tmp
	t.Cleanup(func() {
		*f = original
		_ = tmp.Close()
	})
	return tmp
}

func readFile(t *testing.T, f *os.File) string {
	t.Helper()
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
