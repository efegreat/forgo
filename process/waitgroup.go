package process

import "sync"

// globWg is a global WaitGroup to track goroutines
var globWg = &sync.WaitGroup{}

// Go runs a function in a new goroutine and tracks it
func Go(fn func()) {
	globWg.Add(1)
	go func() {
		defer globWg.Done()
		fn()
	}()
}

// WaitGoroutines waits for all tracked goroutines to finish
func WaitGoroutines() {
	globWg.Wait()
}
