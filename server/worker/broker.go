package worker

import (
	"context"
)

const HeaderRequeueCount = "x-requeue-count"

type IBroker interface {
	Name() string
	// Init initializes the broker with the provided context. Examples of initialization tasks include establishing connections to the broker, setting up necessary resources, and performing any required configuration. This method will be called before creating publishers or subscribers.
	Init(context.Context) error
	// NewDefaultPublisher creates a new publisher instance for sending messages to broker. This is the default publisher for messages that have not set specific publisher name.
	NewDefaultPublisher() (IPublisher, error)
	// NewPublisher creates a new publisher instance for sending messages to the broker. The returned IPublisher can be used to publish messages to the broker.
	NewPublisher(name string) (IPublisher, error)
	// NewSubscriber creates a new subscriber instance for receiving messages from the broker. The returned ISubscriber can be used to consume messages from the broker.
	NewSubscriber(name string, ID string) (ISubscriber, error)
	Close() error
}

type IDelivery interface {
	Message() Message

	// Ack marks the message as successfully processed.
	Ack(ctx context.Context) error

	// Nack marks the message as unsuccessfully processed and
	// makes it eligible for redelivery according to the broker's
	// configured retry/redelivery policy.
	Nack(ctx context.Context) error

	// Reject marks the message as rejected and will not be redelivered.
	Reject(ctx context.Context) error
}

type IPublisher interface {
	// Publish sends a message to the broker for delivery to subscribers. The context can be used to control the lifecycle of the publish operation, allowing for cancellation or timeout of the operation. Publish MUST: be a blocking operation that is intended for single publishing.
	Publish(ctx context.Context, msg Message) error
	Close() error
}

type ISubscriber interface {
	// Consume starts consuming messages from the broker and invokes the provided handler for each message. Consume MUST: block until the context is canceled or an error occurs and respect the values of ConsumerState for concurrency management. Not all brokers support concurrency management, so the implementation should handle this gracefully.
	Consume(ctx context.Context, cs *ConsumerState) error
	Close() error
}
