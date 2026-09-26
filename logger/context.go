package logger

import (
	"context"
	"forgo/operation"
	"os"

	"github.com/rs/zerolog"
)

var (
	// DefaultCtxLogger is the default logger used when no logger is set in the context.
	DefaultCtxLogger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	loggerKey        ctxKey
)

// ctxKey is a type used for the context key to avoid collisions with other context keys.
type ctxKey struct{}

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

// FieldAdder is a function type that takes a zerolog.Context and returns a modified zerolog.Context. It is used to add fields to the logger context.
type FieldAdder func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context

// EventFields is a function type that takes a zerolog.Event and modifies it. It is used to add fields to the logger event.
type EventFields func(ctx *operation.Context, event *zerolog.Event)
