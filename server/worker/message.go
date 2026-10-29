package worker

import (
	"strconv"
	"time"

	"github.com/efegreat/forgo/safemap"

	"github.com/google/uuid"
)

// The headers that carry the identity of a message across the broker. Broker adapters set them when
// they publish a message, and NewMessageFromDelivery reads them back.
const (
	// MessageIDHeader carries Message.ID.
	MessageIDHeader = "x-msg-id"
	// MessageNameHeader carries Message.Name.
	MessageNameHeader = "x-msg-name"
	// MessageTimestampHeader carries Message.CreatedAt, in Unix nanoseconds.
	MessageTimestampHeader = "x-msg-timestamp"
)

// Message is a message the producer publishes and the consumer handles.
type Message struct {
	// ID identifies the message. The inbox counts the attempts of a message and skips the messages
	// it already handled by ID, so it must stay the same across redeliveries.
	ID string
	// Name routes the message to the publisher of the same name in the producer, and to the handler
	// of the same name in the consumer.
	Name string
	// Payload is the body of the message, JSON for the default binder.
	Payload []byte
	// Headers are the string headers the broker carries next to the payload.
	Headers *safemap.JSONMap[string]
	// CreatedAt is when the message was created. The consumer logs the time since then as
	// msg_elapsed. It is zero when the message does not say, and msg_elapsed is then left out.
	CreatedAt time.Time
	// Key is the optional broker-defined routing, partitioning, or ordering key.
	Key string
	// Destination is where the broker publishes the message, such as an exchange or a topic.
	Destination string
}

// NewMessage returns a message for the application to write to its outbox, with a new ID, no headers
// and the current time as CreatedAt. The ID is a UUIDv7, so IDs sort by creation time.
func NewMessage(name string, key string, destination string, payload []byte) Message {
	return Message{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Name:        name,
		Payload:     payload,
		Headers:     safemap.NewJSONMap[string](),
		CreatedAt:   time.Now(),
		Key:         key,
		Destination: destination,
	}
}

// NewMessageFromDelivery rebuilds a message that a broker delivered, for broker adapters. Its ID, name
// and creation time come from the MessageIDHeader, MessageNameHeader and MessageTimestampHeader of
// headers. Nil headers count as none.
//
// Without the ID header, the message gets a new ID, which changes with every delivery: the inbox then
// counts none of its attempts past the first and does not recognize it again. An adapter should set the
// header, when it is missing, to an ID that the broker keeps across redeliveries, such as the broker's
// own message ID. Without the name header, the name is "unknown", and without a valid, positive timestamp,
// CreatedAt is zero.
func NewMessageFromDelivery(key, destination string, payload []byte, headers *safemap.JSONMap[string]) Message {
	if headers == nil {
		headers = safemap.NewJSONMap[string]()
	}
	id, ok := headers.Get(MessageIDHeader)
	if !ok {
		id = uuid.Must(uuid.NewV7()).String()
	}
	name, ok := headers.Get(MessageNameHeader)
	if !ok {
		name = "unknown"
	}
	var createdAt time.Time
	if timestamp, ok := headers.Get(MessageTimestampHeader); ok {
		if nanos, err := strconv.ParseInt(timestamp, 10, 64); err == nil && nanos > 0 {
			createdAt = time.Unix(0, nanos)
		}
	}
	return Message{
		ID:          id,
		Name:        name,
		Payload:     payload,
		Headers:     headers,
		CreatedAt:   createdAt,
		Key:         key,
		Destination: destination,
	}
}
