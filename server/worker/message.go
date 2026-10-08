package worker

import (
	"forgo/types"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const (
	MessageIDHeader        = "x-msg-id"
	MessageNameHeader      = "x-msg-name"
	MessageTimestampHeader = "x-msg-timestamp"
)

type Message struct {
	ID        string
	Name      string
	Payload   []byte
	Headers   *types.JSONMap[string]
	CreatedAt time.Time
	// Key is the optional broker-defined routing, partitioning, or ordering key.
	Key string
	// Destination is the publishing destination
	Destination string
}

func NewMessage(name string, key string, destination string, payload []byte) Message {
	return Message{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Name:        name,
		Payload:     payload,
		Headers:     types.NewJSONMap[string](),
		CreatedAt:   time.Now(),
		Key:         key,
		Destination: destination,
	}
}

func NewMessageFromDelivery(key, destination string, payload []byte, headers *types.JSONMap[string]) Message {
	id, ok := headers.Get(MessageIDHeader)
	if !ok {
		id = uuid.Must(uuid.NewV7()).String()
	}
	name, ok := headers.Get(MessageNameHeader)
	if !ok {
		name = "unknown"
	}
	timestamp, ok := headers.Get(MessageTimestampHeader)
	if !ok {
		timestamp = "0"
	}
	timestampInt, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		timestampInt = 0
	}
	return Message{
		ID:          id,
		Name:        name,
		Payload:     payload,
		Headers:     headers,
		CreatedAt:   time.Unix(0, timestampInt),
		Key:         key,
		Destination: destination,
	}
}
