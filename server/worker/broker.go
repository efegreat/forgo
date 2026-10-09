package worker

import (
	"context"
)

type IBroker interface {
	Name() string
	// NewDefaultPublisher creates a new publisher instance for sending messages to broker. This is the default publisher for messages that have not set specific publisher name.
	NewDefaultPublisher() (IPublisher, error)
	// NewPublisher creates a new publisher instance for sending messages to the broker. The returned IPublisher can be used to publish messages to the broker.
	NewPublisher(name string) (IPublisher, error)
	// NewSubscriber creates a new subscriber instance for receiving messages from the broker. The returned ISubscriber can be used to consume messages from the broker.
	NewSubscriber(name string) (ISubscriber, error)
}

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

type IPublisher interface {
	// Publish sends a message to the broker for delivery to subscribers. The context can be used to control the lifecycle of the publish operation, allowing for cancellation or timeout of the operation. Publish MUST: be a blocking operation that is intended for single publishing.
	Publish(ctx context.Context, msg Message) error
	Close() error
}

type ISubscriber interface {
	// Consume blocks while consuming messages.
	//
	// Implementations must stop accepting new messages when ctx is
	// canceled and must wait for all handlers they started before
	// returning.
	Consume(ctx context.Context, cs ConsumerState) error
	Close() error
}
