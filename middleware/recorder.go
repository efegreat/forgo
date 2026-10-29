package middleware

import (
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

// EventFields adds fields to a line Recorder logs, such as a request's headers and body. Recorder calls
// it only when the line is logged, so a level that drops the line also skips its work, and the operation
// must not rely on its side effects.
type EventFields func(ctx *operation.Context, event *zerolog.Event)

// Recorder logs the start and the end of every operation: "operation started" at info, then
// "operation completed" at info with elapsed, or "operation failed" with err_code, err_severity,
// elapsed and the error. A failure is logged at warn when its severity is light, at error otherwise.
// The code and severity are those of operation.ErrorOf(err). The logged error is the operation's
// error itself, whose text includes the private cause of every operation.Error in it and the text of
// the errors that wrap them.
//
// before adds fields to the start line, and after to the end line. Both run only for lines that are
// logged, so a level above info also skips their work, such as reading and masking a request body.
// When disabled reports true for an operation, nothing is logged for it. Nil functions are skipped.
func Recorder(disabled func(ctx *operation.Context) bool, before EventFields, after EventFields) operation.Middleware {
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

			opErr := operation.ErrorOf(err)
			if opErr.Severity == operation.SeverityLight {
				event = lg.Warn()
			} else {
				event = lg.Error()
			}
			event = event.
				Err(err).
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
