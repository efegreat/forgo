package worker

import (
	"context"
	"errors"
	"fmt"
	"forgo/operation"
	"runtime/debug"
)

const OperationTypeMessageHandler = "message_handler"

var (
	NotFoundHandler = func(IBinder, operation.Transactor, InboxStorage, SubscriberConfig) operation.Handler {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			rErr := st.Delivery.Reject(ctx)
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
			st := ctx.State.(*State)
			key := conf.Key + "-" + subscriberName + "-" + st.Delivery.Message().ID
			return operation.Lock{
				Key: key,
				Ttl: conf.Ttl,
			}
		}
	}
	OnLockConflict = func() func(*operation.Context) error {
		return func(ctx *operation.Context) error {
			st := ctx.State.(*State)
			return st.Delivery.Ack(ctx)
		}
	}
	PanicRecovery = func(sto InboxStorage, sc SubscriberConfig) func(*operation.Context, any) error {
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
	IdempotencyReject = func() func(*operation.Context) error {
		return func(ctx *operation.Context) error {
			d := ctx.State.(*State).Delivery
			return d.Ack(ctx)
		}
	}
)

type HandlerFunc func(context.Context, IDelivery)

// Handler is used to wrap an operation.Handler for message handlers
type Handler func(b IBinder, tx operation.Transactor, sto InboxStorage, sc SubscriberConfig) operation.Handler

// NewHandler creates a Handler for incoming messages
func NewHandler[T any](fn func(context.Context, T) error) Handler {
	return func(b IBinder, tx operation.Transactor, sto InboxStorage, sc SubscriberConfig) operation.Handler {
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
				if rErr := st.Delivery.Reject(ctx); rErr != nil {
					err = fmt.Errorf("%w, rejectErr: %w", err, rErr)
				}
				return operation.Error{
					Code:     "msg_bind",
					Status:   operation.StatusInvalidArgument,
					Message:  "message binding error",
					Severity: operation.SeverityModerate,
				}.Wrap(err)
			}

			handle := func() error {
				if err := fn(ctx, req); err != nil {
					return err
				}
				return sto.MarkCompleted(ctx, msg.ID, sc.Name)
			}

			if tx != nil {
				err := tx.Do(ctx, handle)
				return postHandle(ctx, st, sto, sc, err)
			}
			err = handle()
			return postHandle(ctx, st, sto, sc, err)
		}
	}
}

func postHandle(ctx context.Context, st *State, sto InboxStorage, sc SubscriberConfig, err error) error {
	del := st.Delivery
	// err is nil, send ack.
	if err == nil {
		if ackErr := del.Ack(ctx); ackErr != nil {
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
		if nErr := del.Nack(ctx); nErr != nil {
			return opErr.Wrap(nErr)
		}
		return opErr
	}

	// message is reached maxAttempts
	if err := sto.MarkFailed(ctx, del.Message().ID, sc.Name); err != nil {
		return opErr.Wrap(err)
	}
	if rErr := del.Reject(ctx); rErr != nil {
		return opErr.Wrap(rErr)
	}

	return opErr
}

// State contains worker-specific state associated with an operation.
type State struct {
	Delivery IDelivery
	Attempts int
}
