package operation

import (
	"context"
	"time"
)

var DefaultTolerance = Tolerance{
	Retry:         true,
	RetryAttempts: 5,
	RetryDelay:    5 * time.Second,
}

type Tolerance struct {
	Retry         bool
	RetryAttempts uint
	RetryDelay    time.Duration
	RetryIf       func(error) bool
	OnRetry       func(ctx context.Context, attempt uint, err error)
	OnSuccess     func(context.Context)
	OnFailure     func(ctx context.Context, err error)
}
