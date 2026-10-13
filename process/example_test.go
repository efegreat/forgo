package process_test

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/efegreat/forgo/process"
)

func ExampleGo() {
	process.Go(func() {
		fmt.Println("confirmation sent")
	})

	// App and CLI call it before they close their resources.
	process.WaitGoroutines()
	// Output: confirmation sent
}

func ExampleRunner_TryAsync() {
	// At most 20 mails are sent in the background. Beyond that, the caller sends them itself.
	mails := process.NewRunner(process.NewChannelSemaphore(20))

	var sent atomic.Int32
	for range 50 {
		mails.TryAsync(func() { sent.Add(1) })
	}

	process.WaitGoroutines()
	fmt.Println(sent.Load(), "mails sent")
	// Output: 50 mails sent
}

func ExampleConcurrentRunner() {
	ctx := context.Background()
	runner := process.NewConcurrentRunner(process.NewChannelSemaphore(4))

	var handled atomic.Int32
	for range 10 {
		// Run waits while 4 messages are being handled.
		if err := runner.Run(ctx, func() { handled.Add(1) }); err != nil {
			break // ctx ended
		}
	}

	runner.Wait()
	fmt.Println(handled.Load(), "messages handled")
	// Output: 10 messages handled
}
