package worker

import (
	"forgo/types"
	"time"

	"github.com/google/uuid"
)

const (
	MessageNameHeader      = "x-msg-name"
	MessageTimestampHeader = "x-msg-timestamp"
)

type Message struct {
	ID        string
	Name      string
	Payload   []byte
	Headers   *types.Map
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
		Headers:     types.NewMap(),
		CreatedAt:   time.Now(),
		Key:         key,
		Destination: destination,
	}
}
