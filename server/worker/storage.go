package worker

import (
	"context"
	"time"
)

// OutboxStorage is the outbox the producer relays: the table the application writes its outgoing
// messages to, in the transaction of the change that produced them.
type OutboxStorage interface {
	// Claim must atomically:
	//
	//   1. select eligible rows with FOR UPDATE SKIP LOCKED
	//   2. set processing_id = claim.ID
	//   3. set processing_at = now
	//   4. commit
	//   5. return the claimed messages
	//
	// Eligible messages are:
	//
	//   processed_at IS NULL
	//
	// and either:
	//
	//   processing_id IS NULL
	//
	// or:
	//
	//   processing_at < claim.ExpiredBefore
	//
	Claim(ctx context.Context, claim Claim) ([]Message, error)

	// Complete must atomically:
	//
	//   1. mark successfulIDs as processed
	//   2. clear their claim
	//   3. clear the claim from all remaining messages belonging to claimID
	//
	// This releases failed publications so they can be retried.
	Complete(
		ctx context.Context,
		claimID string,
		successfulIDs []string,
	) error

	// DeleteProcessedBefore must only delete already processed messages.
	DeleteProcessedBefore(
		ctx context.Context,
		before time.Time,
		limit int,
	) (int64, error)
}

// Claim is a batch of outbox messages that one producer instance takes for publishing.
type Claim struct {
	// ID identifies the claim, for Complete.
	ID string

	// Messages with processing_at older than this time may be reclaimed.
	ExpiredBefore time.Time

	// Limit is the number of messages to claim, at most.
	Limit int
}

// InboxStatus is the status of a message in the inbox of a subscriber.
type InboxStatus int8

const (
	// InboxWaiting is a message being handled, or waiting for its next attempt.
	InboxWaiting InboxStatus = iota
	// InboxCompleted is a message that a handler handled.
	InboxCompleted
	// InboxFailed is a message that used up its attempts, or whose handler failed it with ErrNoRetry.
	InboxFailed
	// InboxRejected is a message whose payload the binder rejected.
	InboxRejected
)

// InboxStorage keeps the status and the attempts of every message, per subscriber, so that the consumer
// counts the attempts of a message across deliveries and skips the deliveries of a message it already
// completed, failed or rejected. Its rows are keyed by message ID and subscriber name.
type InboxStorage interface {
	// Status returns the status of the message, and false when it has no row.
	Status(ctx context.Context, messageID, subscriberName string) (InboxStatus, bool, error)
	// MarkCompleted moves the message from waiting to completed. The consumer calls it with the
	// handler's transaction in ctx, and it must join it, so that the handler's writes and the mark
	// commit together. It should fail when the message is no longer waiting, so that a duplicate
	// delivery handled at the same time rolls its writes back instead of committing them twice.
	MarkCompleted(ctx context.Context, messageID, subscriberName string) error
	// IncrementAttempt counts an attempt of a waiting message, and returns the attempts counted so
	// far. The first attempt creates the row, waiting, with 1 attempt. It fails when the message is
	// no longer waiting. The consumer calls it outside the transaction, so that an attempt that
	// rolled back still counts.
	IncrementAttempt(ctx context.Context, messageID, subscriberName string) (current int, err error)
	// MarkFailed moves the message from waiting to failed.
	MarkFailed(ctx context.Context, messageID, subscriberName string) error
	// MarkRejected moves the message from waiting to rejected.
	MarkRejected(ctx context.Context, messageID, subscriberName string) error
	// DeleteBefore deletes at most limit completed and rejected rows last updated before before, and
	// returns how many it deleted. It keeps the failed rows, for inspection and retrying.
	DeleteBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}
