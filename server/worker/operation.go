package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

var (
	ArgKeyRecorderDisabled = operation.ArgKey{}
)

// ErrAlreadySettled is returned by State.Delivery when the delivery was already completed, retried
// or rejected. Only the first settlement reaches the broker: settling a delivery twice is a protocol
// error for some brokers, and RabbitMQ closes the channel on it.
var ErrAlreadySettled = errors.New("worker: delivery already settled")

const (
	OperationTypeMessageHandler = "message_handler"
)

var (
	NotFoundHandler = func(_ IBinder, _ operation.Transactor, _ InboxStorage, sc resolvedSubscriber) operation.Handler {
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
				Ttl: conf.Ttl,
			}
		}
	}
	OnLockConflict = func(ackTimeout time.Duration) func(*operation.Context) error {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			return settle(ctx, ackTimeout, st.Delivery.Complete)
		}
	}
	PanicRecovery = func(sto InboxStorage, sc resolvedSubscriber) func(*operation.Context, any) error {
		return func(ctx *operation.Context, r any) error {
			err := fmt.Errorf("panic: %s, stack: %s", r, string(debug.Stack()))
			st := ctx.State.(*State)
			return postHandle(ctx, st, sto, sc, err)
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

// handlerWrapper is used to wrap an operation.Handler for message handlers
type handlerWrapper func(b IBinder, tx operation.Transactor, sto InboxStorage, sc resolvedSubscriber) operation.Handler

// newHandlerWrapper creates a handlerWrapper for incoming messages
func newHandlerWrapper[T any](fn Handler[T]) handlerWrapper {
	return func(b IBinder, tx operation.Transactor, sto InboxStorage, sc resolvedSubscriber) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			msg := st.Delivery.Message()

			// create/increment attempt
			attempts, err := sto.IncrementAttempt(ctx, msg.ID, sc.Name)
			if err != nil {
				return err
			}
			st.Attempts = attempts

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

			handle := func(ctx context.Context) error {
				if err := fn(ctx, req); err != nil {
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
		if nErr := settle(ctx, sc.AckTimeout, del.Retry); nErr != nil {
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

// settleUnsettled settles a delivery that the operation returned without settling, so that it never
// stays in flight until the broker gives up on it, holding a prefetch slot if the broker has one.
// That happens when the chain fails before postHandle (an inbox, lock or middleware error, a failed
// IncrementAttempt or MarkFailed) or when a middleware returns nil without calling next. Success
// acknowledges the delivery. An error retries it after sc.UnsettledRetryDelay without counting an
// attempt, so that an outage of the inbox or the locker does not become a redelivery loop. ctx ends
// the delay early when the consumer stops.
func settleUnsettled(ctx context.Context, opCtx *operation.Context, d IDelivery, sc resolvedSubscriber, err error) {
	lg := logger.FromCtx(opCtx)
	ack := d.Complete
	if err != nil {
		lg.Error().Err(err).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("message left unsettled, retrying")
		if sc.UnsettledRetryDelay > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(sc.UnsettledRetryDelay):
			}
		}
		ack = d.Retry
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

func (d *onceDelivery) Retry(ctx context.Context) error {
	return d.settle(ctx, d.IDelivery.Retry)
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
