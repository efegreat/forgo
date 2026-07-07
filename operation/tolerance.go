package operation

import (
	"context"
	"time"
)

// DefaultTolerance is the default tolerance configuration for operations.
var DefaultTolerance = Tolerance{
	Retry:         true,
	RetryAttempts: 5,
	RetryDelay:    5 * time.Second,
}

// Tolerance defines the fault tolerance configuration for an operation, including retry behavior and callbacks for success and failure events.
type Tolerance struct {
	Retry         bool
	RetryAttempts uint
	RetryDelay    time.Duration
	RetryIf       func(error) bool
	OnRetry       func(ctx context.Context, attempt uint, err error)
	OnSuccess     func(context.Context)
	OnFailure     func(ctx context.Context, err error)
}
