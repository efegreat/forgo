// Package process runs background work that an App or CLI waits for before it closes its
// resources, with optional limits on how much of it runs at once.
//
// Go starts a tracked goroutine, and WaitGoroutines, which App and CLI call after their servers or
// command stopped, waits for every one of them. Runner starts tracked goroutines up to the limit of
// a semaphore and never waits for a slot. ConcurrentRunner waits for a slot and joins its own
// goroutines with Wait, which suits a loop that must return only after its work finished, such as
// a broker subscriber's Consume. ChannelSemaphore is the in-process ISemaphore both runners take.
//
// See the [Background goroutines] section of REFERENCE.md.
//
// [Background goroutines]: https://github.com/efegreat/forgo/blob/main/REFERENCE.md#background-goroutines
package process
