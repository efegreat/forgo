package operation

import "time"

// DefaultTolerance runs an operation up to 5 times, 5 seconds apart.
var DefaultTolerance = Tolerance{
	Retry:         true,
	RetryAttempts: 5,
	RetryDelay:    5 * time.Second,
}

// Tolerance configures the retries of an operation for the Tolerance middleware.
//
// Every attempt runs with the same *Context, so changes to Args and State carry over to the next
// attempt. Its Context is reset before each attempt, so values that middlewares inside the retries add
// to it do not pile up.
type Tolerance struct {
	// Retry enables retries. Without it, the operation runs once and the other fields are ignored.
	Retry bool
	// RetryAttempts is the number of attempts, the first one included. Zero retries until the
	// operation succeeds, RetryIf refuses its error, or RetryTimeout or the operation's context ends.
	// A cron job's context ends only when the app kills its server, so set RetryTimeout or
	// RetryAttempts for cron jobs.
	RetryAttempts uint
	// RetryDelay is the fixed wait between two attempts. Zero retries at once, so Tolerance{Retry:
	// true}, with neither a delay nor a limit, retries a failing operation in a tight loop.
	RetryDelay time.Duration
	// RetryTimeout bounds the whole run: every attempt runs on a context that ends RetryTimeout after
	// the first attempt started, and no attempt starts after that. Zero means no bound.
	RetryTimeout time.Duration
	// RetryIf reports whether err is worth another attempt. Nil retries every error.
	RetryIf func(err error) bool
	// OnRetry runs right before each retry, with the number of the attempt that failed (from 1) and its
	// error.
	OnRetry func(ctx *Context, attempt uint, err error)
	// OnSuccess runs once an attempt succeeded.
	OnSuccess func(ctx *Context)
	// OnFailure runs once the operation failed for good, with the error it returns.
	OnFailure func(ctx *Context, err error)
}
