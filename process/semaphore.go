package process

import "errors"

var ErrNoSlot = errors.New("no available semaphore slot")

// ISemaphore is an interface that defines the methods for acquiring and releasing a semaphore. It allows for controlling access to a shared resource by multiple goroutines, ensuring that only a limited number of goroutines can access the resource at the same time.
type ISemaphore interface {
	Acquire() error
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

func (s *ChannelSemaphore) Release() error {
	<-s.ch
	return nil
}
