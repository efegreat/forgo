package process

import (
	"context"
	"errors"
)

var (
	ErrNoSlot         = errors.New("no available semaphore slot")
	ErrInvalidRelease = errors.New("semaphore release without an acquire")
)

// ISemaphore is an interface that defines the methods for acquiring and releasing a semaphore. It allows for controlling access to a shared resource by multiple goroutines, ensuring that only a limited number of goroutines can access the resource at the same time.
type ISemaphore interface {
	Acquire() error
	WaitAcquire(context.Context) error
	Release() error
}

// ChannelSemaphore is a struct that implements the ISemaphore interface using a buffered channel.
type ChannelSemaphore struct {
	ch chan struct{}
}

func NewChannelSemaphore(limit int) *ChannelSemaphore {
	return &ChannelSemaphore{
		ch: make(chan struct{}, limit),
	}
}

func (s *ChannelSemaphore) Acquire() error {
	select {
	case s.ch <- struct{}{}:
		return nil
	default:
		return ErrNoSlot
	}
}

func (s *ChannelSemaphore) WaitAcquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ErrNoSlot
	}
}

func (s *ChannelSemaphore) Release() error {
	select {
	case <-s.ch:
		return nil
	default:
		return ErrInvalidRelease
	}
}
