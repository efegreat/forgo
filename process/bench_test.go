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

func BenchmarkRunnerGoOrRun(b *testing.B) {
	r := NewRunner(NewChannelSemaphore(64))
	for b.Loop() {
		r.GoOrRun(func() {})
	}
	WaitGoroutines()
}

func BenchmarkGroupGo(b *testing.B) {
	ctx := context.Background()
	r := NewGroup(NewChannelSemaphore(64))
	for b.Loop() {
		_ = r.Go(ctx, func() {})
	}
	r.Wait()
}
