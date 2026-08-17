package logger

import (
	"context"
	"forgo/types"
	"os"

	"github.com/rs/zerolog"
)

var (
	// DefaultCtxLogger is the default logger used when no logger is set in the context.
	DefaultCtxLogger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	loggerKey        types.CtxKey
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

// FieldAdder is a function type that takes a zerolog.Context and returns a modified zerolog.Context. It is used to add fields to the logger context.
type FieldAdder func(zerolog.Context) zerolog.Context

// MessageAdder is a function type that takes a zerolog.Event and returns a modified zerolog.Event. It is used to add fields to the logger event.
type MessageAdder func(event *zerolog.Event) *zerolog.Event
