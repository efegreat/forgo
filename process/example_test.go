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

func ExampleRunner_GoOrRun() {
	// At most 20 mails are sent in the background. Beyond that, the caller sends them itself.
	mails := process.NewRunner(process.NewChannelSemaphore(20))

	var sent atomic.Int32
	for range 50 {
		mails.GoOrRun(func() { sent.Add(1) })
	}

	process.WaitGoroutines()
	fmt.Println(sent.Load(), "mails sent")
	// Output: 50 mails sent
}

func ExampleGroup() {
	ctx := context.Background()
	group := process.NewGroup(process.NewChannelSemaphore(4))

	var handled atomic.Int32
	for range 10 {
		// Go waits while 4 messages are being handled.
		if err := group.Go(ctx, func() { handled.Add(1) }); err != nil {
			break // ctx ended
		}
	}

	group.Wait()
	fmt.Println(handled.Load(), "messages handled")
	// Output: 10 messages handled
}
