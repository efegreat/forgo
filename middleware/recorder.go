package middleware

import (
	"errors"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// Recorder logs the start and the end of every operation: "operation started" at info, then
// "operation completed" at info with elapsed, or "operation failed" with err_code, err_severity,
// elapsed and the error. A failure is logged at warn when its severity is light, at error otherwise.
// An error that is not an operation.Error is logged as operation.NewFallbackError. The logged error is
// the operation.Error's cause, or its message when it has none, since the cause is private and never
// reaches the client.
//
// before adds fields to the start line, and after to the end line. Both run only for lines that are
// logged, so a level above info also skips their work, such as reading and masking a request body.
// When disabled reports true for an operation, nothing is logged for it. Nil functions are skipped.
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
			cause := opErr.Unwrap()
			if cause == nil {
				// Without a cause, the message is the only reason the line can give.
				cause = opErr
			}
			event = event.
				Err(cause).
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
