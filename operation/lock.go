package operation

import "time"

// Lock represents a lock config for an operation. It contains a key and a time-to-live (TTL) duration. The key is used to identify the lock, and the TTL specifies how long the lock should be held before it expires.
type Lock struct {
	Key string
	Ttl time.Duration
}

// ILocker is an interface that defines the methods for acquiring and releasing locks. It includes methods for locking a key with a specified TTL and unlocking a key.
type ILocker interface {
	Lock(key string, ttl time.Duration) (bool, error)
	Unlock(key string) error
}
