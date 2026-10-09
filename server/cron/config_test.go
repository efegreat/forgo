package cron

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
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
		Lock: operation.Lock{Key: "job", Ttl: time.Minute},
	}}}
	if _, err := cfg.New(r); err == nil || !strings.Contains(err.Error(), "operation.ILocker") {
		t.Fatalf("error %v, want missing locker", err)
	}

	cfg.DisableBaseMiddlewares = true // locks are not applied without base middlewares
	if _, err := cfg.New(r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
