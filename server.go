package forgo

import (
	"context"
	"forgo/operation"
	"sync"
	"time"
)

const DefaultServerTimeout = time.Second * 30

// IServer is an interface that defines the methods that a server should implement. It includes methods for initialization, serving, termination signaling, and termination handling.
type IServer interface {
	Config() ServerConfig
	Name() string
	Init(ctx context.Context) error
	Serve() error
	Terminate(ctx context.Context) error
	Kill()
}

// TermSig is a struct that represents a termination signal. It provides methods to check if the signal has been signaled and to signal it. It uses a mutex to ensure thread-safe access to the signaled state.
type TermSig struct {
	Wait     time.Duration
	mu       sync.RWMutex
	signaled bool
}

// Signaled is a method that checks if the TermSig has been signaled. This is for letting readiness probe know that server is closing.
func (t *TermSig) Signaled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.signaled
}

// Signal is a method that sets the signaled state of the TermSig to true.
func (t *TermSig) Signal() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.signaled = true
}

type ServerConfig struct {
	TermSig                *TermSig
	TermTimeout            time.Duration
	Locker                 operation.ILocker
	Middlewares            []operation.Middleware
	DisableBaseMiddlewares bool
}
