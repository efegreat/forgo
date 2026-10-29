package middleware

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

// BenchmarkBaseChain runs the transport-agnostic part of each server's base chain around a no-op
// handler, logging to io.Discard. The operation.Context it allocates per run counts 2 allocations.
func BenchmarkBaseChain(b *testing.B) {
	locker := &benchLocker{}
	noLock := func(*operation.Context) operation.Lock { return operation.Lock{} }
	lock := func(*operation.Context) operation.Lock { return operation.Lock{Key: "k", TTL: time.Minute} }
	allow := func(*operation.Context) (bool, error) { return true, nil }

	for _, bc := range []struct {
		name  string
		chain []operation.Middleware
	}{
		{"http", []operation.Middleware{ID(), Logger(nil), Recorder(nil, nil, nil), Recovery(nil)}},
		{"cron", []operation.Middleware{ID(), Logger(nil), Locker(locker, lock, nil), Recorder(nil, nil, nil), Tolerance(operation.DefaultTolerance()), Recovery(nil)}},
		{"worker", []operation.Middleware{ID(), Logger(nil), Locker(locker, noLock, nil), Recorder(nil, nil, nil), Recovery(nil), Guard(allow, nil)}},
		{"cli", []operation.Middleware{ID(), Logger(nil), Locker(locker, noLock, nil), Recorder(nil, nil, nil), Recovery(nil)}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			handler := operation.Chain(func(*operation.Context) error { return nil }, bc.chain...)
			base := logger.NewCtx(context.Background(), zerolog.New(io.Discard).With().Str("app_name", "bench").Logger())
			b.ReportAllocs()
			for b.Loop() {
				_ = handler(&operation.Context{Context: base, Name: "op", Type: "bench", Args: new(types.Map[any, any])})
			}
		})
	}
}

type benchLocker struct{}

func (*benchLocker) Lock(context.Context, string, time.Duration) (bool, error) { return true, nil }
func (*benchLocker) WaitLock(context.Context, string, int, time.Duration, time.Duration) (bool, error) {
	return true, nil
}
func (*benchLocker) Unlock(context.Context, string) error { return nil }
