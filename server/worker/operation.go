package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
)

// argKey is the type of the Args keys the consumer reads. Its own type keeps them apart from the keys
// of every other package.
type argKey uint8

const (
	// ArgKeyRecorderDisabled turns the Recorder off for the message handlers whose Args map it to true.
	ArgKeyRecorderDisabled argKey = iota
)

// ErrAlreadySettled is returned by State.Delivery when the delivery was already completed, retried
// or rejected. Only the first settlement reaches the broker: settling a delivery twice is a protocol
// error for some brokers, and RabbitMQ closes the channel on it.
var ErrAlreadySettled = errors.New("worker: delivery already settled")

var (
	// errLocked leaves a delivery whose lock another delivery holds to settleUnsettled, which
	// retries it. Acknowledging it would lose the message when the holder crashed: the broker
	// redelivers a crashed holder's message while its lock has not expired yet.
	errLocked = operation.Error{
		Code:     "msg_locked",
		Status:   operation.StatusAborted,
		Message:  "message is locked by another delivery",
		Severity: operation.SeverityLight,
	}
	// errHandlerTimeout fails a handler that returned after its HandlerConfig.Timeout ran out.
	errHandlerTimeout = operation.Error{
		Code:     "msg_timeout",
		Status:   operation.StatusTimeout,
		Message:  "message handler timed out",
		Severity: operation.SeverityModerate,
	}
	// errAttemptsExhausted fails a message that comes back after its last attempt ended without a
	// verdict, typically because the process crashed or was killed while handling it.
	errAttemptsExhausted = operation.Error{
		Code:     "msg_attempts",
		Status:   operation.StatusAborted,
		Message:  "message has no attempts left",
		Severity: operation.SeverityModerate,
	}
)

const (
	OperationTypeMessageHandler = "message_handler"
)

var (
	NotFoundHandler = func(_ IBinder, _ operation.Transactor, _ InboxStorage, sc resolvedSubscriber, _ time.Duration) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			rErr := settle(ctx, sc.AckTimeout, st.Delivery.Reject)
			return operation.Error{
				Code:     "nf_mh",
				Status:   operation.StatusInvalidArgument,
				Message:  "message handler not found",
				Severity: operation.SeverityModerate,
			}.Wrap(rErr)
		}
	}
	LockCrafter = func(subscriberName string, conf operation.Lock) operation.LockCrafter {
		return func(ctx *operation.Context) operation.Lock {
			if !conf.Enabled() {
				return operation.Lock{}
			}
			st := ctx.State.(*State)
			key := conf.Key + "-" + subscriberName + "-" + st.Delivery.Message().ID
			return operation.Lock{
				Key: key,
				TTL: conf.TTL,
			}
		}
	}
	OnLockConflict = func() func(*operation.Context) error {
		return func(*operation.Context) error {
			return errLocked
		}
	}
	PanicRecovery = func(sto InboxStorage, sc resolvedSubscriber) func(*operation.Context, *middleware.PanicError) error {
		return func(ctx *operation.Context, p *middleware.PanicError) error {
			st := ctx.State.(*State)
			return postHandle(ctx, st, sto, sc, p)
		}
	}
	IdempotencyCheck = func(s InboxStorage, subscriberName string) func(*operation.Context) (bool, error) {
		return func(ctx *operation.Context) (bool, error) {
			d := ctx.State.(*State).Delivery
			st, exist, err := s.Status(ctx, d.Message().ID, subscriberName)
			if err != nil {
				return false, err
			}
			if !exist {
				return true, nil
			}
			return st == InboxWaiting, nil
		}
	}
	IdempotencyReject = func(ackTimeout time.Duration) func(*operation.Context) error {
		return func(ctx *operation.Context) error {
			d := ctx.State.(*State).Delivery
			return settle(ctx, ackTimeout, d.Complete)
		}
	}
	RecorderDisabled = func() func(ctx *operation.Context) bool {
		return func(ctx *operation.Context) bool {
			return ctx.Args.GetTyped[bool](ArgKeyRecorderDisabled)
		}
	}
)

// Handler is a generic type for message handlers. It takes a context and a request of type Req, and returns an error.
type Handler[Req any] func(
	context.Context,
	Req,
) error

type handlerFunc func(context.Context, IDelivery)

// handlerWrapper is used to wrap an operation.Handler for message handlers. timeout bounds the
// handler's context; zero means no limit.
type handlerWrapper func(b IBinder, tx operation.Transactor, sto InboxStorage, sc resolvedSubscriber, timeout time.Duration) operation.Handler

// newHandlerWrapper creates a handlerWrapper for incoming messages
func newHandlerWrapper[T any](fn Handler[T]) handlerWrapper {
	return func(b IBinder, tx operation.Transactor, sto InboxStorage, sc resolvedSubscriber, timeout time.Duration) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			msg := st.Delivery.Message()

			// create/increment attempt
			attempts, err := sto.IncrementAttempt(ctx, msg.ID, sc.Name)
			if err != nil {
				return err
			}
			st.Attempts = attempts
			if attempts > sc.MaxAttempt {
				// Running it again could crash the process again: fail it instead.
				return postHandle(ctx, st, sto, sc, errAttemptsExhausted)
			}

			var req T
			if err := b.Bind(msg, &req); err != nil {
				// If binding fails, reject the message and return an error
				if sErr := sto.MarkRejected(ctx, msg.ID, sc.Name); sErr != nil {
					err = fmt.Errorf("%w, storeErr: %w", err, sErr)
				}
				if rErr := settle(ctx, sc.AckTimeout, st.Delivery.Reject); rErr != nil {
					err = fmt.Errorf("%w, rejectErr: %w", err, rErr)
				}
				return operation.Error{
					Code:     "msg_bind",
					Status:   operation.StatusInvalidArgument,
					Message:  "message binding error",
					Severity: operation.SeverityModerate,
				}.Wrap(err)
			}

			handle := func(ctx *operation.Context) error {
				if err := runHandler(ctx, fn, req, timeout); err != nil {
					return err
				}
				return sto.MarkCompleted(ctx, msg.ID, sc.Name)
			}

			if tx != nil {
				err := tx.Do(ctx, func(txCtx context.Context) error {
					// Run on the transaction's context while handlers keep receiving an *operation.Context.
					txOp := *ctx
					txOp.Context = txCtx
					return handle(&txOp)
				})
				return postHandle(ctx, st, sto, sc, err)
			}
			return postHandle(ctx, st, sto, sc, handle(ctx))
		}
	}
}

// runHandler calls fn on a copy of op whose context ends after timeout, unless timeout is zero. Only
// fn is bound: the inbox mark and the commit that follow keep op's context, so that a handler that
// finished in time is not rolled back by its deadline. An error fn returns once its time ran out
// fails as errHandlerTimeout, unless fn returned an operation.Error of its own.
func runHandler[T any](op *operation.Context, fn Handler[T], req T, timeout time.Duration) error {
	if timeout <= 0 {
		return fn(op, req)
	}
	ctx, cancel := context.WithTimeout(op.Context, timeout)
	defer cancel()
	bounded := *op
	bounded.Context = ctx

	err := fn(&bounded, req)
	var opErr operation.Error
	if err != nil && ctx.Err() != nil && !errors.As(err, &opErr) {
		return errHandlerTimeout.Wrap(err)
	}
	return err
}

// settle sends an acknowledgement to the broker within timeout.
func settle(ctx context.Context, timeout time.Duration, ack func(context.Context) error) error {
	if timeout <= 0 {
		return ack(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return ack(ctx)
}

func postHandle(ctx context.Context, st *State, sto InboxStorage, sc resolvedSubscriber, err error) error {
	del := st.Delivery
	// err is nil, send ack.
	if err == nil {
		if ackErr := settle(ctx, sc.AckTimeout, del.Complete); ackErr != nil {
			return operation.Error{
				Code:     "msg_ack",
				Status:   operation.StatusInternal,
				Message:  "send ack error",
				Severity: operation.SeverityModerate,
			}.Wrap(ackErr)
		}
		return nil
	}

	// convert err to operation.Error
	var opErr operation.Error
	if !errors.As(err, &opErr) {
		opErr = operation.NewFallbackError(err)
	}

	// send nack for redelivery
	if st.Attempts < sc.MaxAttempt {
		if nErr := settle(ctx, sc.AckTimeout, retryFn(del, st.Attempts)); nErr != nil {
			return opErr.Wrap(nErr)
		}
		return opErr
	}

	// message is reached maxAttempts
	if err := sto.MarkFailed(ctx, del.Message().ID, sc.Name); err != nil {
		return opErr.Wrap(err)
	}
	if rErr := settle(ctx, sc.AckTimeout, del.Reject); rErr != nil {
		return opErr.Wrap(rErr)
	}

	return opErr
}

// retryFn returns the Retry of d for the given failed attempt, for settle.
func retryFn(d IDelivery, attempt int) func(context.Context) error {
	return func(ctx context.Context) error {
		return d.Retry(ctx, attempt)
	}
}

// settleUnsettled settles a delivery that the operation returned without settling, so that it never
// stays in flight until the broker gives up on it, holding a prefetch slot if the broker has one.
// That happens when the chain fails before postHandle (an inbox, lock or middleware error, a lock
// held by another delivery, a failed IncrementAttempt or MarkFailed) or when a middleware returns
// nil without calling next. Success acknowledges the delivery. An error retries it without counting
// an attempt, and the broker's retry delay keeps an outage of the inbox or the locker from becoming
// a tight redelivery loop.
func settleUnsettled(opCtx *operation.Context, st *State, sc resolvedSubscriber, err error) {
	lg := logger.FromCtx(opCtx)
	ack := st.Delivery.Complete
	if err != nil {
		var opErr operation.Error
		if !errors.As(err, &opErr) {
			opErr = operation.NewFallbackError(err)
		}
		event := lg.Error()
		if opErr.Severity == operation.SeverityLight {
			event = lg.Warn()
		}
		event.Err(err).
			Str(logger.ErrorCodeLogKey, opErr.Code).
			Int(logger.ErrorSeverityLogKey, int(opErr.Severity)).
			Msg("message left unsettled, retrying")
		ack = retryFn(st.Delivery, st.Attempts)
	}
	// ErrAlreadySettled: the operation settled the delivery meanwhile, from a goroutine it started.
	if sErr := settle(opCtx, sc.AckTimeout, ack); sErr != nil && !errors.Is(sErr, ErrAlreadySettled) {
		lg.Error().Err(sErr).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("failed to settle message")
	}
}

// State contains worker-specific state associated with an operation.
type State struct {
	// Delivery settles at most once: after the first Complete, Retry or Reject, the others return
	// ErrAlreadySettled without reaching the broker. A middleware that drops a message for good
	// settles it itself, for example with Reject. Otherwise the consumer settles it once the chain
	// returns: an error retries it, success acknowledges it.
	Delivery IDelivery
	Attempts int

	// delivery backs Delivery, allocated with State.
	delivery onceDelivery
}

// newState returns a State whose Delivery wraps d in a onceDelivery.
func newState(d IDelivery) *State {
	st := &State{delivery: onceDelivery{IDelivery: d}}
	st.Delivery = &st.delivery
	return st
}

// onceDelivery passes the first settlement of a delivery to the broker and records it.
type onceDelivery struct {
	IDelivery
	settled atomic.Bool
}

func (d *onceDelivery) Complete(ctx context.Context) error {
	return d.settle(ctx, d.IDelivery.Complete)
}

func (d *onceDelivery) Retry(ctx context.Context, attempt int) error {
	return d.settle(ctx, retryFn(d.IDelivery, attempt))
}

func (d *onceDelivery) Reject(ctx context.Context) error {
	return d.settle(ctx, d.IDelivery.Reject)
}

func (d *onceDelivery) settle(ctx context.Context, ack func(context.Context) error) error {
	if !d.settled.CompareAndSwap(false, true) {
		return ErrAlreadySettled
	}
	return ack(ctx)
}
