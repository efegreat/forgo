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
	Init(ctx context.Context) error
	Name() string
	Serve() error
	TermSig() *TermSig
	TermTimeout() time.Duration
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

// ServerOption is a function type that takes a pointer to a Server and modifies its configuration. It is used to provide optional configuration parameters when creating a new Server instance.
type ServerOption func(*Server)

// WithTerminationTimeout is a ServerOption that sets the graceful termination timeout for the server.
func WithTerminationTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.Tt = timeout
	}
}

// WithTermSignal is a ServerOption that sets the termination signaler to be used in readiness probe for the server.
func WithTermSignal(wait time.Duration) ServerOption {
	return func(s *Server) {
		s.ts = &TermSig{Wait: wait}
	}
}

// WithLocker is a ServerOption that sets the locker to be used for locking atomic operations in the server.
func WithLocker(locker operation.ILocker) ServerOption {
	return func(s *Server) {
		s.Locker = locker
	}
}

// WithMiddlewares is a ServerOption that sets the middlewares to be used for the server.
func WithMiddlewares(middlewares ...operation.Middleware) ServerOption {
	return func(s *Server) {
		s.Middlewares = append(s.Middlewares, middlewares...)
	}
}

// WithDisableBaseMiddlewares is a ServerOption that disables the base middlewares for the server.
func WithDisableBaseMiddlewares() ServerOption {
	return func(s *Server) {
		s.DisableBaseMiddlewares = true
	}
}

// Server is the base struct for all servers, providing common functionality such as termination signaling and locking.
type Server struct {
	ts                     *TermSig
	Tt                     time.Duration
	Locker                 operation.ILocker
	Middlewares            []operation.Middleware
	DisableBaseMiddlewares bool
}

func (s *Server) TermSig() *TermSig {
	return s.ts
}

func (s *Server) TermTimeout() time.Duration {
	return s.Tt
}
