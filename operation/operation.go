package operation

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Type string

const (
	TypeCommand Type = "command"
	TypeCronJob Type = "cronJob"
	TypeMessage Type = "message"
)

type IOperation interface {
	ID() string
	Name() string
	Type() Type
	Tolerance() Tolerance
	Lock() Lock
	// BeforeRun is used for pre-run checks and preparations. It does not gain the benefits of Tolerance.
	BeforeRun(ctx context.Context) Error
	Run(ctx context.Context) Error
}

func NewHandler(logger zerolog.Logger, op IOperation, locker ILocker) (func(), error) {
	return func() {
		id := op.ID()
		if id == "" {
			id = uuid.New().String()
		}

		lg := logger.With().
			Str("operation", string(op.Type())).
			Str("name", op.Name()).
			Str("id", id).
			Logger()

		if locker != nil && op.Lock().Key != "" && op.Lock().Ttl != 0 {
			locked, err := locker.Lock(op.Lock().Key, op.Lock().Ttl)
			if err != nil {
				// @todo: emergency log
				lg.Error().Err(err).Msg("failed to acquire operation lock")
				return
			}
			if !locked {
				lg.Info().Msg("operation already running, skipping")
				return
			}
			defer func() {
				if unlockErr := locker.Unlock(op.Lock().Key); unlockErr != nil {
					lg.Error().Err(unlockErr).Msg("failed to release job lock")
					return
				}
			}()
		}

		lg.Info().Msg("operation started")
		now := time.Now()
		//@todo: logger key
		ctx := context.WithValue(context.Background(), "logger", lg)
		errRes := func(err Error) {
			elapsed := time.Since(now)

			switch err.Severity {
			case SeverityLight:
				lg.Warn().
					Err(err.Err).
					Float64("elapsed", elapsed.Seconds()).
					Msg("operation light error")
			case SeverityModerate:
				lg.Error().
					Err(err.Err).
					Float64("elapsed", elapsed.Seconds()).
					Msg("operation moderate error")
			case SeverityCritical:
				//@todo: emergency log
				lg.Error().
					Err(err.Err).
					Float64("elapsed", elapsed.Seconds()).
					Msg("operation critical error")
			}
		}

		if err := op.BeforeRun(ctx); err.Err != nil {
			errRes(err)
			return
		}

		fn := func() error {
			runFn := func() (fnErr error) {
				defer func() {
					if r := recover(); r != nil {
						fnErr = fmt.Errorf("panic: %s, trace: %s", r, string(debug.Stack()))
					}
				}()

				return op.Run(ctx)
			}

			if !op.Tolerance().Retry {
				return runFn()
			}

			return retry.Do(
				runFn,
				retry.Attempts(op.Tolerance().RetryAttempts),
				retry.OnRetry(func(attempt uint, err error) {
					or := op.Tolerance().OnRetry
					if or == nil {
						return
					}
					or(ctx, attempt, err)
				}),
				retry.Delay(op.Tolerance().RetryDelay),
				retry.RetryIf(func(err error) bool {
					ri := op.Tolerance().RetryIf
					if ri == nil {
						// If RetryIf is not defined, we will retry on all errors.
						return true
					}
					return ri(err)
				}),
			)
		}

		err := fn()

		if err != nil {
			of := op.Tolerance().OnFailure
			if of != nil {
				of(ctx, err)
			}
			errRes(Error{Severity: SeverityCritical, Err: err})
			return
		}

		os := op.Tolerance().OnSuccess
		if os != nil {
			os(ctx)
		}

		elapsed := time.Since(now)

		lg.Info().
			Float64("elapsed", elapsed.Seconds()).
			Msg("operation completed successfully")
	}, nil
}
