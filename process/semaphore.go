package process

import "errors"

var ErrNoSlot = errors.New("no available semaphore slot")

type Semaphore interface {
	Acquire() error
	Release() error
}

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
