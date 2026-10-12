package middleware

import (
	"errors"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// Recorder logs the start and the end of every operation: "operation started", then "operation completed",
// or "operation failed" with the error's code and severity, at the warn or error level its severity
// implies. Both end lines carry the elapsed time. before adds fields to the start line and after to the
// end line, and disabled, when it returns true, skips logging for the operation.
//
// before and after run only for events that are logged, so a disabled level also skips their work,
// such as reading and masking a request body.
func Recorder(disabled func(ctx *operation.Context) bool, before logger.EventFields, after logger.EventFields) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if disabled != nil && disabled(ctx) {
				return next(ctx)
			}

			start := time.Now()
			lg := logger.FromCtx(ctx)

			event := lg.Info()
			if before != nil && event.Enabled() {
				before(ctx, event)
			}
			event.Msg("operation started")

			err := next(ctx)

			if err == nil {
				event := lg.Info().Float64(logger.ElapsedTimeLogKey, time.Since(start).Seconds())
				if after != nil && event.Enabled() {
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

			if after != nil && event.Enabled() {
				after(ctx, event)
			}

			event.Msg("operation failed")
			return err
		}
	}
}
