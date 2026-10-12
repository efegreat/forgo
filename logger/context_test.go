package logger

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

type benchKey struct{}

var sinkLogger *zerolog.Logger

func TestFromCtx(t *testing.T) {
	if got := FromCtx(context.Background()); got != defaultLogger.Load() {
		t.Fatal("FromCtx without a logger did not return the default logger")
	}

	var out bytes.Buffer
	lg := zerolog.New(&out)
	ctx := NewCtx(context.Background(), lg)
	ctx = context.WithValue(ctx, benchKey{}, 1)

	if FromCtx(ctx) != FromCtx(ctx) {
		t.Fatal("FromCtx returned different loggers for the same context")
	}
	FromCtx(ctx).Info().Msg("hello")
	if !bytes.Contains(out.Bytes(), []byte("hello")) {
		t.Fatalf("logger in context did not write to its output: %q", out.String())
	}
}

func TestSetDefault(t *testing.T) {
	original := FromCtx(context.Background())

	var first, second, inCtx bytes.Buffer
	restoreFirst := SetDefault(zerolog.New(&first))
	FromCtx(context.Background()).Info().Msg("first")
	FromCtx(NewCtx(context.Background(), zerolog.New(&inCtx))).Info().Msg("in context")

	restoreSecond := SetDefault(zerolog.New(&second))
	restoreFirst() // out of order: the first default was replaced since, so this must not undo the second
	FromCtx(context.Background()).Info().Msg("second")

	restoreSecond()
	restoreFirst()
	if FromCtx(context.Background()) != original {
		t.Fatal("nested restores did not reinstate the original default")
	}

	if got := first.String(); got != `{"level":"info","message":"first"}`+"\n" {
		t.Errorf("first default = %q", got)
	}
	if got := second.String(); got != `{"level":"info","message":"second"}`+"\n" {
		t.Errorf("second default = %q", got)
	}
	if got := inCtx.String(); got != `{"level":"info","message":"in context"}`+"\n" {
		t.Errorf("logger in context = %q, the default must not replace it", got)
	}
}

// TestSetDefaultConcurrent installs defaults while other goroutines log through them, as an App does
// while a resource logs. Run it with -race.
func TestSetDefaultConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				SetDefault(zerolog.New(io.Discard))()
			}
		})
		wg.Go(func() {
			for range 100 {
				FromCtx(context.Background()).Info().Msg("x")
			}
		})
	}
	wg.Wait()
}

// BenchmarkFromCtx looks up a logger stored below a few other context values, as on the request path.
func BenchmarkFromCtx(b *testing.B) {
	ctx := NewCtx(context.Background(), zerolog.New(io.Discard))
	ctx = context.WithValue(ctx, benchKey{}, 1)
	ctx = context.WithValue(ctx, benchKey{}, 2)

	b.ReportAllocs()
	for b.Loop() {
		sinkLogger = FromCtx(ctx)
	}
}
