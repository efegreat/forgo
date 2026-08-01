package logger

import (
	"context"
	"os"

	"github.com/rs/zerolog"
)

// ctxKey is a type used for the context key to avoid collisions with other context keys.
type ctxKey struct{}

var (
	// DefaultCtxLogger is the default logger used when no logger is set in the context.
	DefaultCtxLogger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	loggerKey        ctxKey
)

// NewCtx returns a new context with the logger set.
func NewCtx(ctx context.Context, logger zerolog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// FromCtx returns the logger for context. If not set, returns DefaultCtxLogger
func FromCtx(ctx context.Context) *zerolog.Logger {
	if lg, ok := ctx.Value(loggerKey).(zerolog.Logger); ok {
		return &lg
	}

	return &DefaultCtxLogger
}
