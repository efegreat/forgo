package logger

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/rs/zerolog"
)

type benchKey struct{}

var sinkLogger *zerolog.Logger

func TestFromCtx(t *testing.T) {
	if got := FromCtx(context.Background()); got != &DefaultCtxLogger {
		t.Fatal("FromCtx without a logger did not return DefaultCtxLogger")
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
