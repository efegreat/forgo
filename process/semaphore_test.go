package process

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestNewChannelSemaphoreLimit(t *testing.T) {
	tests := []struct {
		limit int
		slots int
	}{
		{limit: 3, slots: 3},
		{limit: 1, slots: 1},
		{limit: 0, slots: 1},
		{limit: -2, slots: 1},
	}
	for _, tt := range tests {
		s := NewChannelSemaphore(tt.limit)
		for i := range tt.slots {
			if err := s.Acquire(); err != nil {
				t.Fatalf("limit %d: Acquire %d: %v", tt.limit, i+1, err)
			}
		}
		if err := s.Acquire(); err != ErrNoSlot {
			t.Fatalf("limit %d: Acquire beyond %d slots = %v, want ErrNoSlot", tt.limit, tt.slots, err)
		}
	}
}

func TestChannelSemaphoreReleaseFreesASlot(t *testing.T) {
	s := NewChannelSemaphore(2)
	for range 2 {
		if err := s.Acquire(); err != nil {
			t.Fatal(err)
		}
	}
	s.Release()
	if err := s.Acquire(); err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	if err := s.Acquire(); err != ErrNoSlot {
		t.Fatalf("Acquire = %v, want ErrNoSlot", err)
	}
}

func TestChannelSemaphoreReleaseWithoutAcquirePanics(t *testing.T) {
	s := NewChannelSemaphore(1)
	if err := s.Acquire(); err != nil {
		t.Fatal(err)
	}
	s.Release()

	defer func() {
		if recover() == nil {
			t.Fatal("Release without a taken slot did not panic")
		}
	}()
	s.Release()
}

func TestChannelSemaphoreWaitAcquireWaitsForRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewChannelSemaphore(1)
		if err := s.WaitAcquire(t.Context()); err != nil {
			t.Fatalf("WaitAcquire with a free slot: %v", err)
		}

		result := make(chan error, 1)
		go func() { result <- s.WaitAcquire(t.Context()) }()
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("WaitAcquire returned %v while no slot was free", err)
		default:
		}

		s.Release()
		synctest.Wait()
		if err := <-result; err != nil {
			t.Fatalf("WaitAcquire after Release: %v", err)
		}
	})
}

// select picks at random between ready cases, so a free slot must not win over an ended context.
func TestChannelSemaphoreWaitAcquireEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s := NewChannelSemaphore(1)
	for range 1000 {
		err := s.WaitAcquire(ctx)
		if !errors.Is(err, ErrNoSlot) || !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitAcquire with an ended context = %v, want ErrNoSlot and context.Canceled", err)
		}
	}
	if err := s.Acquire(); err != nil {
		t.Fatalf("a failed WaitAcquire took the slot: %v", err)
	}
}

func TestChannelSemaphoreWaitAcquireContextEndsWhileWaiting(t *testing.T) {
	tests := []struct {
		name string
		ctx  func(context.Context) (context.Context, context.CancelFunc)
		want error
	}{
		{
			name: "deadline",
			ctx: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithTimeout(parent, time.Second)
			},
			want: context.DeadlineExceeded,
		},
		{
			name: "cancel",
			ctx: func(parent context.Context) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(parent)
				time.AfterFunc(time.Second, cancel)
				return ctx, cancel
			},
			want: context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := NewChannelSemaphore(1)
				if err := s.Acquire(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := tt.ctx(t.Context())
				defer cancel()

				start := time.Now()
				err := s.WaitAcquire(ctx)
				if !errors.Is(err, ErrNoSlot) || !errors.Is(err, tt.want) {
					t.Fatalf("WaitAcquire = %v, want ErrNoSlot and %v", err, tt.want)
				}
				if waited := time.Since(start); waited != time.Second {
					t.Fatalf("WaitAcquire returned after %v, want 1s", waited)
				}
			})
		})
	}
}
