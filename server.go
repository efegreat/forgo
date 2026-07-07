package forgo

import (
	"context"
	"forgo/operation"
	"sync"
	"time"
)

// ServerName is a type that represents the name of a server. It can be used to identify different types of servers in the application.
type ServerName string

const (
	ServerNameHttp    ServerName = "http"
	ServerNameCommand ServerName = "command"
	ServerNameCron    ServerName = "cron"
	ServerNameWorker  ServerName = "worker"
)

// IServer is an interface that defines the methods that a server should implement. It includes methods for initialization, serving, termination signaling, and termination handling.
type IServer interface {
	Init(ctx context.Context) error
	Name() ServerName
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
		s.tt = timeout
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
		s.locker = locker
	}
}

var DefaultTimeout = time.Second * 10

// Server is the base struct for all servers, providing common functionality such as termination signaling and locking.
type Server struct {
	tt     time.Duration
	ts     *TermSig
	locker operation.ILocker
}

func NewServer(opts ...ServerOption) *Server {
	s := &Server{}
	for _, opt := range opts {
		opt(s)
	}
	if s.tt == 0 {
		s.tt = DefaultTimeout
	}

	return s
}

func (s *Server) TermSig() *TermSig {
	return s.ts
}

func (s *Server) TermTimeout() time.Duration {
	return s.tt
}
