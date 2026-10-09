package forgo

import (
	"context"
	"sync"
	"time"

	"github.com/efegreat/forgo/operation"
)

const defaultServerTimeout = time.Second * 30

// IServer is an interface that defines the methods that a server should implement. It includes methods for initialization, serving, termination signaling, and termination handling.
type IServer interface {
	Term() TermConfig
	Name() string
	Init(ctx context.Context) error
	Serve() error
	Terminate(ctx context.Context) error
	Kill()
}

type ServerFactory interface {
	Name() string
	New(r Resolver) (IServer, error)
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

type TermConfig struct {
	TermSig     *TermSig
	TermTimeout time.Duration
}

type ServerConfig struct {
	Term                   TermConfig
	Middlewares            []MiddlewareFactory
	DisableBaseMiddlewares bool
	resolvedMiddlewares    []operation.Middleware
}

func (s ServerConfig) Sanitize() ServerConfig {
	if s.Term.TermTimeout == 0 {
		s.Term.TermTimeout = defaultServerTimeout
	}
	return s
}

func (s ServerConfig) ResolveMiddlewares(r Resolver) (ServerConfig, error) {
	middlewares, err := ResolveMiddlewares(r, s.Middlewares)
	if err != nil {
		return ServerConfig{}, err
	}
	s.resolvedMiddlewares = middlewares
	return s, nil
}

func (s ServerConfig) ResolvedMiddlewares() []operation.Middleware {
	return s.resolvedMiddlewares
}
