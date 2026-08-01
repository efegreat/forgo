package operation

import (
	"context"
	"fmt"
	"forgo/logger"
	"runtime/debug"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

// IOperation represents an operation that can be executed with pre-run checks, execution, fault tolerance and error handling.
type IOperation interface {
	ID() string
	Name() string
	Type() string
	Tolerance() Tolerance
	Lock() Lock
	// BeforeRun is used for pre-run checks and preparations. It does not gain the benefits of Tolerance.
	BeforeRun(ctx context.Context) Error
	Run(ctx context.Context) Error
}

// NewHandler creates a new operation handler function that executes the given operation with logging, locking, and error handling.
func NewHandler(baseLogger zerolog.Logger, locker ILocker, op IOperation) func() {
	return func() {
		id := op.ID()
		if id == "" {
			id = xid.New().String()
		}

		lg := baseLogger.With().
			Str(logger.OperationTypeLogKey, op.Type()).
			Str(logger.OperationNameLogKey, op.Name()).
			Str(logger.OperationIDLogKey, id).
			Logger()

		if locker != nil && op.Lock().Key != "" && op.Lock().Ttl != 0 {
			locked, err := locker.Lock(op.Lock().Key, op.Lock().Ttl)
			if err != nil {
				lg.Error().
					Err(err).
					Int(logger.ErrorSeverityLogKey, int(SeverityCritical)).
					Msg("failed to acquire operation lock")
				return
			}
			if !locked {
				lg.Info().Msg("operation already running, skipping")
				return
			}
			defer func() {
				if unlockErr := locker.Unlock(op.Lock().Key); unlockErr != nil {
					lg.Error().
						Err(unlockErr).
						Int(logger.ErrorSeverityLogKey, int(SeverityCritical)).
						Msg("failed to release job lock")
					return
				}
			}()
		}

		lg.Info().Msg("operation started")
		now := time.Now()
		ctx := logger.NewCtx(context.Background(), lg)
		errRes := func(err Error) {
			elapsed := time.Since(now)
			lg := lg.With().
				Err(err.Err).
				Float64(logger.ElapsedTimeLogKey, elapsed.Seconds()).
				Int(logger.ErrorSeverityLogKey, int(err.Severity)).
				Logger()

			if err.Severity == SeverityLight {
				lg.Warn().Msg("operation light error")
				return
			}

			lg.Error().Msg("operation error")
		}

		if err := op.BeforeRun(ctx); err.Err != nil {
			errRes(err)
			return
		}

		fn := func() Error {
			runFn := func() (fnErr Error) {
				defer func() {
					if r := recover(); r != nil {
						fnErr = Error{Severity: SeverityCritical, Err: fmt.Errorf("panic: %s, trace: %s", r, string(debug.Stack()))}
					}
				}()

				return op.Run(ctx)
			}

			if !op.Tolerance().Retry {
				return runFn()
			}

			err := retry.Do(
				func() error {
					return runFn().Err
				},
				retry.Attempts(op.Tolerance().RetryAttempts),
				retry.OnRetry(func(attempt uint, err error) {
					or := op.Tolerance().OnRetry
					if or == nil {
						return
					}
					or(ctx, attempt, err)
				}),
				retry.Delay(op.Tolerance().RetryDelay),
				retry.DelayType(retry.FixedDelay),
				retry.RetryIf(func(err error) bool {
					ri := op.Tolerance().RetryIf
					if ri == nil {
						// If RetryIf is not defined, we will retry on all errors.
						return true
					}
					return ri(err)
				}),
			)

			if err != nil {
				return Error{Severity: SeverityCritical, Err: err}
			}

			return Error{}
		}

		err := fn()
		if err.Err != nil {
			of := op.Tolerance().OnFailure
			if of != nil {
				of(ctx, err)
			}
			errRes(err)
			return
		}

		os := op.Tolerance().OnSuccess
		if os != nil {
			os(ctx)
		}

		elapsed := time.Since(now)

		lg.Info().
			Float64(logger.ElapsedTimeLogKey, elapsed.Seconds()).
			Msg("operation completed successfully")
	}
}
