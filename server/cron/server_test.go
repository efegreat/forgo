package cron

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

// logBuffer collects the logs of runs, which the scheduler writes from its goroutines.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// every is a schedule faster than robfig's, whose @every rounds up to a second.
type every time.Duration

func (e every) Next(t time.Time) time.Time { return t.Add(time.Duration(e)) }

// fastServer returns an initialized server with one job that runs every 10ms.
func fastServer(t *testing.T, job IJob, logs *logBuffer) *Server {
	t.Helper()
	s := newServer(t, Config{Jobs: []JobConfig{{Name: "job", Schedule: "@daily", Job: As(func() IJob { return job })}}}, resolver(t))
	s.cfg.jobs[0].Schedule = every(10 * time.Millisecond)
	if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(logs))); err != nil {
		t.Fatal(err)
	}
	return s
}

// serve runs Serve in a goroutine and returns its result.
func serve(s *Server) <-chan error {
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	return served
}

func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func TestServeRunsJobsUntilTerminate(t *testing.T) {
	ran := make(chan struct{}, 1)
	s := fastServer(t, funcJob(func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	}), new(logBuffer))
	served := serve(s)

	receive(t, ran, "a run")
	if err := s.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, served, "Serve"); err != nil {
		t.Fatalf("Serve returned %v", err)
	}
}

// TestTerminateBeforeServe checks that Serve returns when the app terminates before it started, for
// example because another server failed to start.
func TestTerminateBeforeServe(t *testing.T) {
	var runs atomic.Int32
	s := fastServer(t, funcJob(func(context.Context) error { runs.Add(1); return nil }), new(logBuffer))

	if err := s.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := receive(t, serve(s), "Serve"); err != nil {
		t.Fatalf("Serve returned %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := runs.Load(); n != 0 {
		t.Fatalf("%d runs after Terminate", n)
	}
}

func TestTerminateWaitsForRunningJobs(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	jobErr := make(chan error, 1)
	s := fastServer(t, funcJob(func(ctx context.Context) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		jobErr <- ctx.Err()
		return nil
	}), new(logBuffer))
	served := serve(s)
	receive(t, started, "a run")

	terminated := make(chan error, 1)
	go func() { terminated <- s.Terminate(context.Background()) }()
	if err := receive(t, served, "Serve"); err != nil {
		t.Fatalf("Serve returned %v", err)
	}
	select {
	case <-terminated:
		t.Fatal("Terminate returned before the running job")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := receive(t, jobErr, "the job"); err != nil {
		t.Fatalf("the job's context ended during Terminate: %v", err)
	}
	if err := receive(t, terminated, "Terminate"); err != nil {
		t.Fatalf("Terminate returned %v", err)
	}
}

// TestTerminateTimeoutLeavesJobsToKill checks that Terminate never cancels a running job, even when its
// context ends first, and that Kill does.
func TestTerminateTimeoutLeavesJobsToKill(t *testing.T) {
	started := make(chan struct{}, 1)
	cancelled := make(chan struct{})
	s := fastServer(t, funcJob(func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}), new(logBuffer))
	serve(s)
	receive(t, started, "a run")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Terminate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Terminate returned %v, want a deadline error", err)
	}
	select {
	case <-cancelled:
		t.Fatal("Terminate cancelled the running job")
	case <-time.After(50 * time.Millisecond):
	}

	s.Kill()
	receive(t, cancelled, "the job's cancellation")
}

// TestKillEndsRetryWaits checks that Kill stops a failing job between attempts, instead of letting
// its Tolerance wait for the next one.
func TestKillEndsRetryWaits(t *testing.T) {
	attempted := make(chan struct{}, 1)
	cfg := Config{Jobs: []JobConfig{{
		Name: "job", Schedule: "@daily",
		Tolerance: operation.Tolerance{Retry: true, RetryAttempts: 5, RetryDelay: time.Hour},
		Job: As(func() IJob {
			return funcJob(func(context.Context) error { attempted <- struct{}{}; return errors.New("down") })
		}),
	}}}
	s := initServer(t, cfg, new(logBuffer), resolver(t))
	finished := make(chan struct{})
	go func() { s.createRun(s.cfg.jobs[0])(); close(finished) }()
	receive(t, attempted, "the first attempt")

	s.Kill()
	receive(t, finished, "the run")
}

func TestChainOrder(t *testing.T) {
	var order []string
	mw := func(name string) forgo.MiddlewareFactory {
		return forgo.MiddlewareAs(func() operation.Middleware {
			return func(next operation.Handler) operation.Handler {
				return func(ctx *operation.Context) error {
					order = append(order, name)
					return next(ctx)
				}
			}
		})
	}
	var got *operation.Context
	for _, disabled := range []bool{false, true} {
		order, got = nil, nil
		logs := new(logBuffer)
		cfg := Config{
			ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{mw("server")}, DisableBaseMiddlewares: disabled},
			Jobs: []JobConfig{{
				Name: "job", Schedule: "@daily",
				Middlewares: []forgo.MiddlewareFactory{mw("job")},
				Job: As(func() IJob {
					return funcJob(func(ctx context.Context) error {
						order = append(order, "run")
						got, _ = ctx.(*operation.Context)
						return nil
					})
				}),
			}},
		}
		initRun(t, cfg, logs, resolver(t))()

		if strings.Join(order, ",") != "server,job,run" {
			t.Fatalf("disabled=%v: order %v", disabled, order)
		}
		if got == nil || got.Type != OperationTypeCronJob || got.Name != "job" {
			t.Fatalf("disabled=%v: job received %+v", disabled, got)
		}
		if logged := strings.Contains(logs.String(), "operation completed"); logged == disabled || (got.ID == "") != disabled {
			t.Fatalf("disabled=%v: base chain applied wrongly: ID %q, logs:\n%s", disabled, got.ID, logs)
		}
	}
}

func TestToleranceRetriesFailedRuns(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		var attempts int
		logs := new(logBuffer)
		cfg := Config{
			ServerConfig: forgo.ServerConfig{DisableBaseMiddlewares: disabled},
			Jobs: []JobConfig{{
				Name: "job", Schedule: "@daily",
				Tolerance: operation.Tolerance{Retry: true, RetryAttempts: 3},
				Job: As(func() IJob {
					return funcJob(func(context.Context) error {
						if attempts++; attempts < 3 {
							return errors.New("flaky")
						}
						return nil
					})
				}),
			}},
		}
		initRun(t, cfg, logs, resolver(t))()

		want := 3
		if disabled {
			want = 1 // the base chain applies Tolerance
		}
		if attempts != want {
			t.Fatalf("disabled=%v: %d attempts, want %d", disabled, attempts, want)
		}
		if !disabled && strings.Count(logs.String(), "operation completed") != 1 {
			t.Fatalf("want one completed run:\n%s", logs)
		}
	}
}

func TestRecoveryRecordsPanics(t *testing.T) {
	logs := new(logBuffer)
	cfg := Config{Jobs: []JobConfig{{
		Name: "job", Schedule: "@daily",
		Job: As(func() IJob { return funcJob(func(context.Context) error { panic("boom") }) }),
	}}}
	initRun(t, cfg, logs, resolver(t))()

	if out := logs.String(); !strings.Contains(out, "operation failed") || !strings.Contains(out, "panic: boom") {
		t.Fatalf("panic not recorded as a failed run:\n%s", out)
	}
}

type failingServer struct{}

func (failingServer) Name() string                              { return "failing" }
func (failingServer) New(forgo.Resolver) (forgo.IServer, error) { return failingServer{}, nil }
func (failingServer) Term() forgo.TermConfig                    { return forgo.TermConfig{} }
func (failingServer) Init(context.Context) error                { return nil }
func (failingServer) Serve() error                              { return errors.New("port taken") }
func (failingServer) Terminate(context.Context) error           { return nil }
func (failingServer) Kill()                                     {}

// TestAppStopsWhenAnotherServerFails checks that an app whose other server fails at once stops without
// waiting for the cron server's TermTimeout, however the goroutines of Serve are scheduled.
func TestAppStopsWhenAnotherServerFails(t *testing.T) {
	cron := Config{
		ServerConfig: forgo.ServerConfig{Term: forgo.TermConfig{TermTimeout: time.Minute}},
		Jobs:         []JobConfig{{Name: "job", Schedule: "@daily", Job: As(newNoopJob)}},
	}
	for range 20 {
		app := forgo.NewApp(forgo.WithLogger(logger.Config{Output: io.Discard}), forgo.WithServers(cron, failingServer{}))
		done := make(chan error, 1)
		go func() { done <- app.Run() }()
		if err := receive(t, done, "App.Run"); err == nil || !strings.Contains(err.Error(), "port taken") {
			t.Fatalf("Run returned %v, want the serve error", err)
		}
	}
}
