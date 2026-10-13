package forgo

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/efegreat/forgo/operation"
)

const defaultServerTimeout = time.Second * 30

// IServer is a long-running unit of an App, such as an HTTP server. App drives it through this lifecycle:
//
//	Init → Serve (in its own goroutine) → Terminate → Kill, when Terminate times out
type IServer interface {
	// Term returns the termination settings. App reads them when it terminates the server.
	Term() TermConfig
	// Name is the server_name of the server's logs.
	Name() string
	// Init prepares what Serve needs, and must neither block nor start goroutines: when another server
	// fails to start, App returns without terminating the servers it initialized. ctx carries the
	// server's logger, and Init stores it to derive every operation's context from it.
	Init(ctx context.Context) error
	// Serve serves until Terminate or Kill stops it, then returns nil. An error stops the whole app.
	Serve() error
	// Terminate stops the server gracefully: it makes Serve return and waits for the work in flight and
	// every goroutine the server started. App counts the server as terminated once both Terminate and
	// Serve returned. ctx carries the server's logger and expires after TermConfig.TermTimeout.
	Terminate(ctx context.Context) error
	// Kill force-stops the server when Terminate and Serve did not return within TermTimeout. App does
	// not wait for the server after Kill.
	Kill()
}

// ServerFactory creates a server from configuration. Built-in server configs, such as http.Config,
// implement it with value receivers, so they can be declared as package variables.
type ServerFactory interface {
	// Name names the server in the errors of New.
	Name() string
	// New resolves everything the server needs through r, such as handlers and middlewares, because r is
	// sealed before Serve.
	New(r Resolver) (IServer, error)
}

// TermSig tells a server's readiness probe that the server is terminating, such as http.HealthHandler.
// App signals it when termination starts, then waits for Wait before calling Terminate, so that load
// balancers stop sending traffic while the server still serves it. Use it through a pointer.
type TermSig struct {
	// Wait is how long App waits between Signal and Terminate.
	Wait     time.Duration
	signaled atomic.Bool
}

// Signaled reports whether termination has started. It is safe for concurrent use.
func (t *TermSig) Signaled() bool {
	return t.signaled.Load()
}

// Signal marks the start of termination. It is safe for concurrent use.
func (t *TermSig) Signal() {
	t.signaled.Store(true)
}

// TermConfig is how App terminates a server.
type TermConfig struct {
	// TermSig, when set, is signaled before Terminate. See TermSig.
	TermSig *TermSig
	// TermTimeout is how long Terminate and Serve may take to return before Kill. Zero or less means
	// 30 seconds.
	TermTimeout time.Duration
}

// ServerConfig holds the settings every server shares. Server configs embed it, and their New calls
// Sanitize and ResolveMiddlewares.
type ServerConfig struct {
	Term TermConfig
	// Middlewares run on every operation of the server, after the base middlewares and before narrower
	// ones, such as those of a route.
	Middlewares []MiddlewareFactory
	// DisableBaseMiddlewares leaves out the server's built-in chain, such as ID, Logger and Recorder.
	DisableBaseMiddlewares bool

	resolvedMiddlewares []operation.Middleware
}

// Sanitize returns a copy of s with defaults applied: a TermTimeout of 30 seconds.
func (s ServerConfig) Sanitize() ServerConfig {
	if s.Term.TermTimeout <= 0 {
		s.Term.TermTimeout = defaultServerTimeout
	}
	return s
}

// ResolveMiddlewares returns a copy of s that holds the middlewares of Middlewares, resolved with r. Read
// them with ResolvedMiddlewares.
func (s ServerConfig) ResolveMiddlewares(r Resolver) (ServerConfig, error) {
	middlewares, err := ResolveMiddlewares(r, s.Middlewares)
	if err != nil {
		return ServerConfig{}, err
	}
	s.resolvedMiddlewares = middlewares
	return s, nil
}

// ResolvedMiddlewares returns the middlewares ResolveMiddlewares resolved, in order.
func (s ServerConfig) ResolvedMiddlewares() []operation.Middleware {
	return s.resolvedMiddlewares
}
