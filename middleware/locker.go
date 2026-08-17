package middleware

import (
	"fmt"
	"forgo/logger"
	"forgo/operation"
)

// Locker is a middleware that acquires a lock before executing the next handler. If the lock cannot be acquired, it logs an error and skips the execution of the next handler. After the next handler is executed, it releases the lock. It takes an ILocker implementation and a Lock configuration as parameters.
func Locker(l operation.ILocker, conf operation.Lock) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if conf.Key == "" || conf.Ttl == 0 {
				return next(ctx)
			}
			lg := logger.FromCtx(ctx.Context)
			locked, err := l.Lock(conf.Key, conf.Ttl)
			if err != nil {
				return operation.Error{
					Severity: operation.SeverityCritical,
					Err:      fmt.Errorf("failed to acquire lock: %w", err),
				}
			}
			if !locked {
				lg.Info().Msg("operation already running, skipping")
				return nil
			}
			defer func() {
				if unlockErr := l.Unlock(conf.Key); unlockErr != nil {
					lg.Error().
						Err(unlockErr).
						Int(logger.ErrorSeverityLogKey, int(operation.SeverityCritical)).
						Msg("failed to release job lock")
				}
			}()
			return next(ctx)
		}
	}
}
