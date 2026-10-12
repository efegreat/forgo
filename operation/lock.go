package operation

import (
	"context"
	"time"
)

// Lock configures the distributed lock of an operation. Key identifies the lock across processes, and
// TTL bounds how long it is held: the lock expires on its own after TTL, even when the process holding
// it crashed.
//
// Nothing renews a lock while its operation runs, so TTL must cover the longest run, including every
// retry of a Tolerance that runs under the lock.
type Lock struct {
	Key string
	TTL time.Duration
}

// Enabled reports whether the lock is configured. A lock without a key or with a non-positive TTL is not applied.
func (l Lock) Enabled() bool {
	return l.Key != "" && l.TTL > 0
}

// ILocker acquires and releases distributed locks for the Locker middleware.
type ILocker interface {
	// Lock tries once to acquire key for ttl, without waiting for another holder to release it. It
	// returns false and a nil error when another holder has the lock, and an error only when it cannot
	// tell, for example because its backend is unreachable.
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Unlock releases key. It must release only a lock this locker acquired: once the TTL ran out,
	// another process may hold the key, and Unlock then leaves it alone and returns an error.
	Unlock(ctx context.Context, key string) error
}

// LockCrafter returns the lock of an operation. It runs for every operation, so the key can depend on
// the operation, as the worker's per-message keys do. A Lock that is not Enabled means no lock.
type LockCrafter func(ctx *Context) Lock
