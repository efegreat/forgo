package process

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoSlot is returned by ISemaphore.Acquire when every slot is taken. WaitAcquire returns it
// joined with the context's error, so errors.Is matches both.
var ErrNoSlot = errors.New("process: no free semaphore slot")

// ISemaphore limits how many holders run at once. Runner and Group take one, so a single limit
// can cover several of them, and another implementation can replace ChannelSemaphore.
type ISemaphore interface {
	// Acquire takes a slot without waiting, and returns ErrNoSlot when none is free.
	Acquire() error
	// WaitAcquire takes a slot, waiting for one to free up. Once ctx ended, it fails even when a
	// slot is free, with an error matching both ErrNoSlot and ctx.Err().
	WaitAcquire(ctx context.Context) error
	// Release frees a slot taken by Acquire or WaitAcquire. Releasing a slot that was not taken is
	// a programming error.
	Release()
}

// ChannelSemaphore is an ISemaphore backed by a buffered channel. It is safe for concurrent use.
type ChannelSemaphore struct {
	ch chan struct{}
}

// NewChannelSemaphore returns a semaphore with limit slots. A limit below 1 means 1.
func NewChannelSemaphore(limit int) *ChannelSemaphore {
	return &ChannelSemaphore{
		ch: make(chan struct{}, max(limit, 1)),
	}
}

// Acquire takes a slot without waiting, and returns ErrNoSlot when none is free.
func (s *ChannelSemaphore) Acquire() error {
	select {
	case s.ch <- struct{}{}:
		return nil
	default:
		return ErrNoSlot
	}
}

// WaitAcquire takes a slot, waiting for one to free up. Once ctx ended, it fails even when a slot
// is free, with an error matching both ErrNoSlot and ctx.Err().
func (s *ChannelSemaphore) WaitAcquire(ctx context.Context) error {
	// Checked first, since select picks at random when a slot is free as well.
	select {
	case <-ctx.Done():
		return noSlot(ctx)
	default:
	}
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return noSlot(ctx)
	}
}

// Release frees a slot taken by Acquire or WaitAcquire. It panics when no slot is taken. A release
// too many while another holder has a slot frees that holder's slot instead, which it cannot tell.
func (s *ChannelSemaphore) Release() {
	select {
	case <-s.ch:
	default:
		panic("process: semaphore released more often than acquired")
	}
}

func noSlot(ctx context.Context) error {
	return fmt.Errorf("%w: %w", ErrNoSlot, ctx.Err())
}
