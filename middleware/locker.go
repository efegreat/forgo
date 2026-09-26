package middleware

import (
	"fmt"
	"forgo/logger"
	"forgo/operation"
)

// Locker is a middleware that acquires a lock before executing the next handler. If the lock cannot be acquired, it logs an error and skips the execution of the next handler. After the next handler is executed, it releases the lock. It takes an ILocker implementation and a Lock configuration as parameters.
func Locker(l operation.ILocker, lc operation.LockCrafter, onConflict func(*operation.Context) error) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if lc == nil {
				return next(ctx)
			}
			conf := lc(ctx)
			if conf.Key == "" || conf.Ttl == 0 {
				return next(ctx)
			}
			lg := logger.FromCtx(ctx.Context)
			acquired, err := l.Lock(conf.Key, conf.Ttl)
			if err != nil {
				return fmt.Errorf("failed to acquire lock: %w", err)
			}
			if !acquired {
				lg.Info().Msg("operation already running, skipping")
				if onConflict != nil {
					return onConflict(ctx)
				}
				return nil
			}
			defer func() {
				if unlockErr := l.Unlock(conf.Key); unlockErr != nil {
					lg.Error().
						Err(unlockErr).
						Int(logger.ErrorSeverityLogKey, int(operation.SeverityCritical)).
						Msg("failed to release operation lock")
				}
			}()
			return next(ctx)
		}
	}
}
