package cron

import (
	"bytes"
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

func TestLockRequiresLocker(t *testing.T) {
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer boot.Close()

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
func (downLocker) Unlock(context.Context, string) error { return nil }

// TestLockErrorIsLogged checks that a job whose lock cannot be acquired leaves a log line: cron
// discards the result of a run, and Locker sits outside Recorder.
func TestLockErrorIsLogged(t *testing.T) {
	boot := forgo.NewBootstrap(nil, []any{func() operation.ILocker { return downLocker{} }})
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer boot.Close()

	ran := false
	cfg := Config{Jobs: []JobConfig{{
		Name: "locked", Schedule: "@every 1m",
		Job:  As(func() IJob { return funcJob(func() { ran = true }) }),
		Lock: operation.Lock{Key: "job", TTL: time.Minute},
	}}}
	srv, err := cfg.New(r)
	if err != nil {
		t.Fatal(err)
	}
	s := srv.(*Server)

	var logs bytes.Buffer
	s.createHandler(logger.NewCtx(context.Background(), zerolog.New(&logs)), s.cfg.jobs[0])()

	if ran {
		t.Fatal("job ran without its lock")
	}
	if !strings.Contains(logs.String(), `"error":"redis down"`) || !strings.Contains(logs.String(), "failed to acquire operation lock") {
		t.Fatalf("lock failure not logged:\n%s", &logs)
	}
}

type funcJob func()

func (funcJob) Tolerance() operation.Tolerance { return operation.Tolerance{} }
func (f funcJob) Run(context.Context) error    { f(); return nil }
