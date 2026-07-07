package forgo

import (
	"context"
	"fmt"
	"forgo/operation"
	"time"
)

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

type MessageOperation struct {
	d             IDelivery
	h             IMessageHandler
	beforeRunFunc func(ctx context.Context) operation.Error
	onSuccessFunc func(ctx context.Context)
	onFailureFunc func(ctx context.Context, err error)
}

func (m MessageOperation) ID() string {
	return m.d.Message().ID
}

func (m MessageOperation) Name() string {
	return m.h.Name()
}

func (m MessageOperation) Type() operation.Type {
	return operation.TypeMessage
}

func (m MessageOperation) Tolerance() operation.Tolerance {
	return operation.Tolerance{
		OnSuccess: m.onSuccessFunc,
		OnFailure: m.onFailureFunc,
	}
}

func (m MessageOperation) Lock() operation.Lock {
	return operation.Lock{
		Key: m.d.Message().ID,
		Ttl: time.Hour,
	}
}

func (m MessageOperation) BeforeRun(ctx context.Context) operation.Error {
	return m.beforeRunFunc(ctx)
}

func (m MessageOperation) Run(ctx context.Context) operation.Error {
	err := m.h.Handle(ctx, m.d)
	return operation.Error{
		Severity: err.Severity,
		Err:      err,
	}
}
