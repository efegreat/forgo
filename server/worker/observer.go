package worker

import (
	"context"
	"time"

	"github.com/efegreat/forgo/operation"
)

// SettlementKind is what a delivery was settled with at the broker.
type SettlementKind uint8

const (
	// SettlementCompleted is an acknowledgement: the broker drops the message.
	SettlementCompleted SettlementKind = iota + 1
	// SettlementRetried hands the message back to the broker for a redelivery.
	SettlementRetried
	// SettlementRejected drops the message for good, to the dead-letter queue when the broker has one.
	SettlementRejected
)

// String returns the name of k, such as "retried", for metric labels. A value outside the declared kinds
// is "unknown".
func (k SettlementKind) String() string {
	switch k {
	case SettlementCompleted:
		return "completed"
	case SettlementRetried:
		return "retried"
	case SettlementRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// Settlement is how a delivery was settled, as the consumer reports it to an IDeliveryObserver.
type Settlement struct {
	Kind SettlementKind
	// Attempt is State.Attempts when the delivery was settled: 0 when the inbox did not count the attempt,
	// such as for a redelivery the idempotency guard settled, or a delivery whose lock another one held.
	Attempt int
	// Duplicate reports a redelivery that the idempotency guard settled from the inbox status of its
	// message, without running its handler.
	Duplicate bool
	// Err is the broker's failure to take the settlement, nil when it took it.
	Err error
}

// IDeliveryObserver is an operation.IObserver that is also told how the consumer settled every delivery,
// for metrics. The settlement of a delivery is not the outcome of its operation: a failed operation can be
// retried or rejected, and the consumer settles the deliveries the chain left unsettled after the chain.
//
// Settled is called exactly once per delivery, when its settlement reached the broker, from the goroutine
// that settled it: the operation's, or one the handler started. ctx is the delivery's operation, whose
// State is a *State. It must be safe for concurrent use and must not block.
type IDeliveryObserver interface {
	Settled(ctx *operation.Context, s Settlement)
}

// RelayBatch is a batch of the outbox relay, as the producer reports it to an IRelayObserver.
type RelayBatch struct {
	// Claimed is the number of messages the batch claimed from the outbox, 0 for an empty batch.
	Claimed int
	// Published is the number of claimed messages the broker took. The others go back to the outbox, for
	// a later batch.
	Published int
	// Elapsed is the time the batch took, from its claim to its completion.
	Elapsed time.Duration
	// Err is the failure to claim or to complete the batch, nil otherwise. Failed publishes are not
	// errors of the batch: they are the claimed messages that were not published.
	Err error
}

// IBacklogObserver is an operation.IObserver that is also told the backlog of the outbox, for gauges of
// how many messages wait to be published and for how long. The producer samples it every
// ProducerConfig.BacklogInterval when its OutboxStorage is an OutboxBacklogStorage.
//
// Every producer instance samples the same outbox, so dashboards aggregate the instances with max, not sum.
// BacklogSampled runs on the sampling goroutine, so it must not block.
type IBacklogObserver interface {
	BacklogSampled(ctx context.Context, b OutboxBacklog)
}

// IRelayObserver is an operation.IObserver that is also told about every batch of the producer's outbox
// relay, for metrics. Batches are not operations, so nothing else observes them.
//
// Relayed is called for every batch, the empty ones included, so that a relay that stopped can be told from
// one with nothing to publish. It runs on the relay's goroutine, so it must not block.
type IRelayObserver interface {
	Relayed(ctx context.Context, b RelayBatch)
}
