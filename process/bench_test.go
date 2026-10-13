package process

import (
	"context"
	"testing"
)

func BenchmarkGo(b *testing.B) {
	for b.Loop() {
		Go(func() {})
	}
	WaitGoroutines()
}

func BenchmarkChannelSemaphore(b *testing.B) {
	ctx := context.Background()
	s := NewChannelSemaphore(1)
	b.Run("Acquire", func(b *testing.B) {
		for b.Loop() {
			_ = s.Acquire()
			s.Release()
		}
	})
	b.Run("WaitAcquire", func(b *testing.B) {
		for b.Loop() {
			_ = s.WaitAcquire(ctx)
			s.Release()
		}
	})
}

func BenchmarkRunnerTryAsync(b *testing.B) {
	r := NewRunner(NewChannelSemaphore(64))
	for b.Loop() {
		r.TryAsync(func() {})
	}
	WaitGoroutines()
}

func BenchmarkConcurrentRunnerRun(b *testing.B) {
	ctx := context.Background()
	r := NewConcurrentRunner(NewChannelSemaphore(64))
	for b.Loop() {
		_ = r.Run(ctx, func() {})
	}
	r.Wait()
}
