//go:build race

package worker

// raceEnabled skips allocation checks, whose counts the race detector changes, such as by dropping
// pooled items at random.
const raceEnabled = true
