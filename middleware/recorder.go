package middleware

import (
	"errors"
	"forgo/logger"
	"forgo/operation"
	"time"
)

// Recorder is a middleware that records the start and end of an operation, along with the elapsed time. It takes two functions that modify the zerolog.Context for the start and end of the operation, respectively. This allows for customizing the log messages for each operation.
func Recorder(before logger.MessageAdder, after logger.MessageAdder) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			start := time.Now()
			lg := logger.FromCtx(ctx.Context)
			before(lg.Info()).Msg("operation started")

			err := next(ctx)

			if err == nil {
				after(lg.Info().Float64(logger.ElapsedTimeLogKey, time.Since(start).Seconds())).Msg("operation completed")
				return nil
			}

			var opErr operation.Error
			if errors.As(err, &opErr) {
				//
			} else {
				opErr = operation.Error{
					Severity: operation.SeverityCritical,
					Err:      err,
				}
			}

			after(lg.Error().
				Err(opErr.Err).
				Float64(logger.ElapsedTimeLogKey, time.Since(start).Seconds()).
				Int(logger.ErrorSeverityLogKey, int(opErr.Severity))).Msg("operation failed")
			return err
		}
	}
}
