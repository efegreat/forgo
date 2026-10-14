package cron

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

type noopJob struct{}

func (noopJob) Tolerance() operation.Tolerance { return operation.Tolerance{} }
func (noopJob) Run(context.Context) error      { return nil }

func newNoopJob() IJob { return noopJob{} }

// funcJob runs a function, without retries.
type funcJob func(ctx context.Context) error

func (funcJob) Tolerance() operation.Tolerance  { return operation.Tolerance{} }
func (f funcJob) Run(ctx context.Context) error { return f(ctx) }

// retryJob runs a function with a tolerance.
type retryJob struct {
	tolerance operation.Tolerance
	run       func(ctx context.Context) error
}

func (j retryJob) Tolerance() operation.Tolerance { return j.tolerance }
func (j retryJob) Run(ctx context.Context) error  { return j.run(ctx) }

// resolver returns a built bootstrap's resolver, closed at the end of the test.
func resolver(t *testing.T, providers ...any) forgo.Resolver {
	t.Helper()
	boot := forgo.NewBootstrap(nil, providers)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	return r
}

type dependency struct{}

func TestNewRejectsInvalidJobs(t *testing.T) {
	failing := forgo.MiddlewareAs(func(dependency) operation.Middleware { return nil })
	tests := []struct {
		name string
		jobs []JobConfig
		want string
	}{
		{
			name: "no name",
			jobs: []JobConfig{{Schedule: "@daily", Job: As(newNoopJob)}},
			want: "cron: job 0 has no name",
		},
		{
			name: "no job",
			jobs: []JobConfig{{Name: "a", Schedule: "@daily"}},
			want: `cron: job "a" has no Job`,
		},
		{
			name: "duplicate name",
			jobs: []JobConfig{
				{Name: "a", Schedule: "@daily", Job: As(newNoopJob)},
				{Name: "a", Schedule: "@hourly", Job: As(newNoopJob)},
			},
			want: `cron: duplicate job "a"`,
		},
		{
			name: "invalid schedule",
			jobs: []JobConfig{{Name: "a", Schedule: "* * *", Job: As(newNoopJob)}},
			want: `cron: job "a": schedule "* * *": expected exactly 5 fields`,
		},
		{
			name: "empty schedule",
			jobs: []JobConfig{{Name: "a", Job: As(newNoopJob)}},
			want: `cron: job "a": schedule "": empty spec string`,
		},
		{
			name: "unresolvable job",
			jobs: []JobConfig{{Name: "a", Schedule: "@daily", Job: As(func(dependency) IJob { return noopJob{} })}},
			want: `cron: job "a": `,
		},
		{
			name: "unresolvable middleware",
			jobs: []JobConfig{{Name: "a", Schedule: "@daily", Middlewares: []forgo.MiddlewareFactory{failing}, Job: As(newNoopJob)}},
			want: `cron: job "a": `,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Config{Jobs: tt.jobs}.New(resolver(t))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNewAcceptsSchedules(t *testing.T) {
	for _, schedule := range []string{"*/5 * * * *", "0 3 * * MON-FRI", "@daily", "@every 30s", "CRON_TZ=Europe/Istanbul 0 3 * * *"} {
		if _, err := (Config{Jobs: []JobConfig{{Name: "a", Schedule: schedule, Job: As(newNoopJob)}}}).New(resolver(t)); err != nil {
			t.Errorf("schedule %q: %v", schedule, err)
		}
	}
}

func TestNameAndTerm(t *testing.T) {
	cfg := Config{}
	if cfg.Name() != ServerNameCron {
		t.Fatalf("Config.Name() = %q", cfg.Name())
	}
	srv, err := cfg.New(resolver(t))
	if err != nil {
		t.Fatal(err)
	}
	if srv.Name() != ServerNameCron {
		t.Fatalf("Server.Name() = %q", srv.Name())
	}
	if got := srv.Term().TermTimeout; got != 30*time.Second {
		t.Fatalf("TermTimeout %v, want the 30s default", got)
	}
}

func TestLockRequiresLocker(t *testing.T) {
	r := resolver(t)
	cfg := Config{Jobs: []JobConfig{{
		Name: "locked", Schedule: "@every 1m", Job: As(newNoopJob),
		Lock: operation.Lock{Key: "job", TTL: time.Minute},
	}}}
	if _, err := cfg.New(r); err == nil || !strings.Contains(err.Error(), "operation.ILocker") {
		t.Fatalf("error %v, want missing locker", err)
	}

	cfg.DisableBaseMiddlewares = true // locks are not applied without base middlewares
	if _, err := cfg.New(r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

type downLocker struct{}

func (downLocker) Lock(context.Context, string, time.Duration) (bool, error) {
	return false, errors.New("redis down")
}
func (l downLocker) WaitLock(ctx context.Context, key string, _ int, _, ttl time.Duration) (bool, error) {
	return l.Lock(ctx, key, ttl)
}
func (downLocker) Unlock(context.Context, string) error { return nil }

// TestLockErrorIsLogged checks that a job whose lock cannot be acquired leaves a log line: cron
// discards the result of a run, and Locker sits outside Recorder.
func TestLockErrorIsLogged(t *testing.T) {
	ran := false
	cfg := Config{Jobs: []JobConfig{{
		Name: "locked", Schedule: "@every 1m",
		Job:  As(func() IJob { return funcJob(func(context.Context) error { ran = true; return nil }) }),
		Lock: operation.Lock{Key: "job", TTL: time.Minute},
	}}}
	logs := new(logBuffer)
	run := initRun(t, cfg, logs, resolver(t, func() operation.ILocker { return downLocker{} }))

	run()

	if ran {
		t.Fatal("job ran without its lock")
	}
	if !strings.Contains(logs.String(), `"error":"redis down"`) || !strings.Contains(logs.String(), "failed to acquire operation lock") {
		t.Fatalf("lock failure not logged:\n%s", logs)
	}
}

// newServer returns the server cfg creates, before Init.
func newServer(t *testing.T, cfg Config, r forgo.Resolver) *server {
	t.Helper()
	srv, err := cfg.New(r)
	if err != nil {
		t.Fatal(err)
	}
	return srv.(*server)
}

// initServer returns the server cfg creates, initialized with a logger that writes to w.
func initServer(t *testing.T, cfg Config, w *logBuffer, r forgo.Resolver) *server {
	t.Helper()
	s := newServer(t, cfg, r)
	if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(w))); err != nil {
		t.Fatal(err)
	}
	return s
}

// initRun returns the function the scheduler calls for a run of the first job of cfg.
func initRun(t *testing.T, cfg Config, w *logBuffer, r forgo.Resolver) func() {
	t.Helper()
	s := initServer(t, cfg, w, r)
	return s.createRun(s.cfg.jobs[0])
}
