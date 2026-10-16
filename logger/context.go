package logger

import (
	"context"
	"os"
	"sync/atomic"

	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

var (
	// defaultLogger is the logger of contexts without one. It is swapped atomically, since App.Run and
	// CLI.Run install theirs while other code may log.
	defaultLogger atomic.Pointer[zerolog.Logger]
	loggerKey     ctxKey
)

func init() {
	lg := zerolog.New(os.Stdout).With().Timestamp().Logger()
	defaultLogger.Store(&lg)
}

// ctxKey is a type used for the context key to avoid collisions with other context keys.
type ctxKey struct{}

// NewCtx returns a new context with the logger set. It allocates once, for a context that holds the logger
// itself.
func NewCtx(ctx context.Context, logger zerolog.Logger) context.Context {
	return &loggerCtx{Context: ctx, lg: logger}
}

// loggerCtx is context.WithValue(ctx, loggerKey, &lg) with lg held inline, so that NewCtx does not
// allocate a copy of the logger besides the context. Like the contexts of WithValue, it forwards every
// other key, which keeps the cancellation of the contexts derived from it as efficient.
type loggerCtx struct {
	context.Context
	lg zerolog.Logger
}

func (c *loggerCtx) Value(key any) any {
	if _, ok := key.(ctxKey); ok {
		return &c.lg
	}
	return c.Context.Value(key)
}

// FromCtx returns the logger of ctx, or the default logger (see SetDefault) when ctx has none.
//
// The returned logger is shared by every reader of the context, which keeps FromCtx free of allocations.
// Derive a new logger with With() instead of mutating it (UpdateContext, Hook, ...).
func FromCtx(ctx context.Context) *zerolog.Logger {
	if lg, ok := ctx.Value(loggerKey).(*zerolog.Logger); ok {
		return lg
	}

	return defaultLogger.Load()
}

// SetDefault makes lg the logger FromCtx returns for a context without one, such as the
// context.Background() of a constructor. Until then it is a logger of JSON lines to stdout, at every level.
//
// App.Run and CLI.Run install their logger for as long as they run, so that resources, providers and
// code that logs without an operation's context still use the configured outputs, level and fields.
//
// The returned function reinstates the default lg replaced, if lg is still the default, so it never
// undoes a later SetDefault. Restores nest: call them in the reverse order of their SetDefault.
// SetDefault and restore are safe for concurrent use with FromCtx.
func SetDefault(lg zerolog.Logger) (restore func()) {
	current := &lg
	previous := defaultLogger.Swap(current)
	return func() {
		defaultLogger.CompareAndSwap(current, previous)
	}
}

// FieldAdder adds fields to the logger middleware.Logger derives for each operation, so that every line
// logged during the operation carries them. It runs once per operation, on the request path, so it
// should only read what the transport already holds, such as the operation's State.
type FieldAdder func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context

// EventFields adds fields to a line middleware.Recorder logs, such as a request's headers and body.
// Recorder calls it only when the line is logged, so a level that drops the line also skips its work,
// and the operation must not rely on its side effects.
type EventFields func(ctx *operation.Context, event *zerolog.Event)
