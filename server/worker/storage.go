package worker

import (
	"context"
	"time"
)

// OutboxStorage abstracts the database implementation.
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

type Claim struct {
	ID string

	// Messages with processing_at older than this time may be reclaimed.
	ExpiredBefore time.Time

	Limit int
}

type InboxStatus int8

const (
	InboxWaiting InboxStatus = iota
	InboxCompleted
	InboxFailed
	InboxRejected
)

type InboxStorage interface {
	Status(ctx context.Context, messageID, subscriberName string) (InboxStatus, bool, error)
	MarkCompleted(ctx context.Context, messageID, subscriberName string) error
	IncrementAttempt(ctx context.Context, messageID, subscriberName string) (current int, err error)
	MarkFailed(ctx context.Context, messageID, subscriberName string) error
	MarkRejected(ctx context.Context, messageID, subscriberName string) error
	DeleteBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}
