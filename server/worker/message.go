package worker

import (
	"forgo/types"
	"time"

	"github.com/google/uuid"
)

const (
	MessageNameHeader      = "x-msg-name"
	MessageTimestampHeader = "x-msg-timestamp"
	MessagePublisherHeader = "x-msg-publisher"
)

type Message struct {
	ID        string
	Name      string
	Payload   []byte
	Headers   *types.Map
	CreatedAt time.Time

	// Key is the publishing key
	Key string
	// Topic is the publishing destination
	Topic string
}

func NewMessage(name string, key string, topic string, payload []byte) Message {
	return Message{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Name:      name,
		Payload:   payload,
		Headers:   types.NewMap(),
		CreatedAt: time.Now(),
		Key:       key,
		Topic:     topic,
	}
}
