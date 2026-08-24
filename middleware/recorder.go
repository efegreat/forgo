package middleware

import (
	"errors"
	"forgo/logger"
	"forgo/operation"
	"time"
)

// Recorder is a middleware that records the start and end of an operation, along with the elapsed time. It takes two functions that modify the zerolog.Context for the start and end of the operation, respectively. This allows for customizing the log messages for each operation.
func Recorder(before logger.EventFields, after logger.EventFields) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			start := time.Now()
			lg := logger.FromCtx(ctx.Context)

			event := lg.Info()
			if before != nil {
				before(ctx, event)
			}
			event.Msg("operation started")

			err := next(ctx)

			if err == nil {
				event := lg.Info().Float64(logger.ElapsedTimeLogKey, time.Since(start).Seconds())
				if after != nil {
					after(ctx, event)
				}

				event.Msg("operation completed")
				return nil
			}

			var opErr operation.Error
			if !errors.As(err, &opErr) {
				opErr = operation.NewFallbackError(err)
			}

			if opErr.Severity == operation.SeverityLight {
				event = lg.Warn()
			} else {
				event = lg.Error()
			}
			event = event.
				Err(opErr.Unwrap()).
				Str(logger.ErrorCodeLogKey, opErr.Code).
				Float64(logger.ElapsedTimeLogKey, time.Since(start).Seconds()).
				Int(logger.ErrorSeverityLogKey, int(opErr.Severity))

			if after != nil {
				after(ctx, event)
			}

			event.Msg("operation failed")
			return err
		}
	}
}
