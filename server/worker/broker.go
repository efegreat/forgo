package worker

import (
	"context"
)

// IBroker creates the publishers and the subscribers of a message broker. Provide it as a resource whose
// Value is the IBroker.
type IBroker interface {
	// Name names the broker in logs, under broker.
	Name() string
	// NewDefaultPublisher creates a publisher for the producer's DefaultPublisher, which publishes the
	// messages that no named publisher takes. The producer creates DefaultPublisher.Concurrency of them.
	NewDefaultPublisher() (IPublisher, error)
	// NewPublisher creates a publisher for the producer's publisher of that name. The producer creates
	// its Concurrency of them.
	NewPublisher(name string) (IPublisher, error)
	// NewSubscriber creates the subscriber of that name, for the consumer's SubscriberConfig of the same
	// name.
	NewSubscriber(name string) (ISubscriber, error)
}

// IDelivery is a message a subscriber received, and its acknowledgement to the broker. The consumer
// settles every delivery exactly once, with Complete, Retry or Reject.
type IDelivery interface {
	Message() Message

	// Complete marks the message as successfully processed.
	Complete(ctx context.Context) error

	// Retry marks the message as unsuccessfully processed and makes it eligible for redelivery.
	// attempt is the attempt that failed, as counted by the inbox. It is 0 when processing failed
	// before the attempt was counted, such as on a lock conflict or an inbox outage. The broker
	// derives the delay before the redelivery from it, according to its own configuration, and
	// redelivers at once when it has none.
	Retry(ctx context.Context, attempt int) error

	// Reject marks the message as rejected and will not be redelivered.
	Reject(ctx context.Context) error
}

// IPublisher publishes messages to the broker. The producer uses each one from one goroutine at a time.
type IPublisher interface {
	// Publish sends msg to the broker and returns once the broker accepted it, or failed to. ctx bounds
	// it.
	Publish(ctx context.Context, msg Message) error
	// Close releases the publisher. The producer calls it once it stopped publishing.
	Close() error
}

// ISubscriber receives the messages of one subscriber from the broker.
type ISubscriber interface {
	// Consume blocks while consuming messages, and calls cs.Handler for each one, with at most
	// cs.Concurrency calls at once. The context it passes to cs.Handler may carry values of the
	// broker, such as a trace.
	//
	// When ctx is canceled, Consume must stop accepting new messages, wait for all handlers it
	// started and return nil. It returns an error when it stops consuming on its own, for example
	// once the broker gave up reconnecting, which stops the app.
	Consume(ctx context.Context, cs ConsumerState) error
	// Close releases the subscriber. The consumer calls it once Consume returned.
	Close() error
}
