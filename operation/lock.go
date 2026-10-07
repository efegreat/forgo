package operation

import (
	"context"
	"time"
)

// Lock represents a lock config for an operation. It contains a key and a time-to-live (TTL) duration. The key is used to identify the lock, and the TTL specifies how long the lock should be held before it expires.
type Lock struct {
	Key string
	Ttl time.Duration
}

// Enabled reports whether the lock is configured. A lock without a key or with a non-positive TTL is not applied.
func (l Lock) Enabled() bool {
	return l.Key != "" && l.Ttl > 0
}

// ILocker is an interface that defines the methods for acquiring and releasing locks. It includes methods for locking a key with a specified TTL and unlocking a key.
type ILocker interface {
	Lock(key string, ttl time.Duration) (bool, error)
	WaitLock(ctx context.Context, key string, attempts int, delay, ttl time.Duration) (bool, error)
	Unlock(key string) error
}

// LockCrafter is used to craft the lock params with Context params
type LockCrafter func(ctx *Context) Lock
