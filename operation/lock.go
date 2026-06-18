package operation

import "time"

type Lock struct {
	Key string
	Ttl time.Duration
}

type ILocker interface {
	Lock(key string, ttl time.Duration) (bool, error)
	Unlock(key string) error
}
