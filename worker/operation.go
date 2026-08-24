package worker

import (
	"context"
	"fmt"
	"forgo/operation"
)

const OperationTypeMessage = "message"

type Message struct {
	ID      string
	Key     string
	Payload []byte
	Headers map[string]string
}

type IDelivery interface {
	Message() Message
	Ack(ctx context.Context) error
	Nack(ctx context.Context) error
}

type IPublisher interface {
	Publish(ctx context.Context, msg Message) error
}

type ISubscriber interface {
	Consume(ctx context.Context, h IMessageHandler) error
}

type RequeueType int

const (
	NoRequeue RequeueType = iota
	RequeueWithLimit
	RequeueLimitless
)

type MessageError struct {
	Severity operation.ErrorSeverity
	Requeue  RequeueType
	Err      error
}

func (e MessageError) Error() string {
	if e.Err != nil {
		return fmt.Errorf("severity: %d, requeue: %d, error: %w", e.Severity, e.Requeue, e.Err).Error()
	}
	return ""
}

func (e MessageError) Unwrap() error {
	return e.Err
}

type IMessageHandler interface {
	Name() string
	Handle(ctx context.Context, m IDelivery) MessageError
}

type IStream interface {
	IPublisher
	ISubscriber
}
