package middleware

import (
	"context"
	"errors"
	"fmt"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// ErrNoLocker is returned when an operation requests a lock but no operation.ILocker is configured.
var ErrNoLocker = errors.New("operation lock requested but no locker is configured")

// Locker runs the rest of the chain under the lock that crafter returns for the operation, so that only
// one operation at a time, across every process sharing the locker, runs with that key.
//
// A nil crafter, or a Lock that is not Enabled, runs the operation without a lock. When another holder
// has the lock, Locker logs it at info and returns what onConflict returns, or nil without onConflict,
// so the operation is skipped. When the lock cannot be acquired, Locker logs the failure and returns
// it, and the operation does not run. Locker logs these failures itself because it sits outside
// Recorder, and a cron run has nobody else to log them.
//
// The lock is released once the rest of the chain returns, even after a panic or the end of the
// operation's context. A failed release is logged at critical severity. Nothing renews the lock in the
// meantime, so its TTL must cover the longest run.
//
// With a nil locker, an operation whose Lock is enabled fails with ErrNoLocker. Servers check this in
// New, so that a missing locker fails at startup instead.
func Locker(l operation.ILocker, crafter operation.LockCrafter, onConflict func(*operation.Context) error) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		if crafter == nil {
			return next
		}
		return func(ctx *operation.Context) error {
			conf := crafter(ctx)
			if !conf.Enabled() {
				return next(ctx)
			}
			if l == nil {
				return fmt.Errorf("%w: %s", ErrNoLocker, conf.Key)
			}
			lg := logger.FromCtx(ctx)
			acquired, err := l.Lock(ctx, conf.Key, conf.TTL)
			if err != nil {
				lg.Error().
					Err(err).
					Int(logger.ErrorSeverityLogKey, int(operation.SeverityCritical)).
					Msg("failed to acquire operation lock")
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
				// The operation's context may have ended, and the lock must still be released.
				if unlockErr := l.Unlock(context.WithoutCancel(ctx), conf.Key); unlockErr != nil {
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
