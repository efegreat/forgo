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

// ILocker acquires and releases distributed locks, for the Locker middleware and for application code.
type ILocker interface {
	// Lock tries once to acquire key for ttl, without waiting for another holder to release it. It
	// returns false and a nil error when another holder has the lock, and an error only when it cannot
	// tell, for example because its backend is unreachable.
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// WaitLock tries to acquire key for ttl up to attempts times, waiting delay between tries but not
	// after the last one. Fewer than one attempt means one. It returns false and a nil error when
	// every try found the lock held, and ctx's error when ctx ends first. The Locker middleware never
	// waits for a lock; WaitLock is for application code that must.
	WaitLock(ctx context.Context, key string, attempts int, delay, ttl time.Duration) (bool, error)
	// Unlock releases key. It must release only a lock this locker acquired: once the TTL ran out,
	// another process may hold the key, and Unlock then leaves it alone and returns an error.
	Unlock(ctx context.Context, key string) error
}

// LockCrafter returns the lock of an operation. It runs for every operation, so the key can depend on
// the operation, as the worker's per-message keys do. A Lock that is not Enabled means no lock.
type LockCrafter func(ctx *Context) Lock

// ILockObserver is an IObserver that is also told when the Locker middleware of a server could not lock an
// operation, for metrics. Servers wrap their ILocker with ObserveLocks to report to it.
//
// key can hold a message ID, as the worker's per-message keys do, so it must not become a metric label:
// label by the operation of ctx, which operation.FromCtx finds. The hooks run on the operation's goroutine,
// so they must be safe for concurrent use and must not block.
type ILockObserver interface {
	// LockHeld is called when another holder had the lock, so the operation was skipped or retried.
	LockHeld(ctx context.Context, key string)
	// LockFailed is called when the lock could not be acquired, with the locker's error.
	LockFailed(ctx context.Context, key string, err error)
}

// ObserveLocks returns l, reporting the locks its Lock did not acquire to o when o implements
// ILockObserver. It returns l itself when l is nil or o does not implement it, so that a server can wrap
// the ILocker it resolved in New either way. WaitLock and Unlock report nothing.
func ObserveLocks(l ILocker, o IObserver) ILocker {
	lo, ok := o.(ILockObserver)
	if l == nil || !ok {
		return l
	}
	return observedLocker{ILocker: l, observer: lo}
}

// observedLocker reports what its Lock did not acquire to an ILockObserver.
type observedLocker struct {
	ILocker
	observer ILockObserver
}

func (l observedLocker) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	acquired, err := l.ILocker.Lock(ctx, key, ttl)
	switch {
	case err != nil:
		l.observer.LockFailed(ctx, key, err)
	case !acquired:
		l.observer.LockHeld(ctx, key)
	}
	return acquired, err
}
