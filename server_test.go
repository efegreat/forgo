package forgo

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
)

func TestServerConfigSanitize(t *testing.T) {
	tests := []struct {
		timeout time.Duration
		want    time.Duration
	}{
		{0, defaultServerTimeout},
		{-time.Second, defaultServerTimeout},
		{time.Second, time.Second},
	}
	for _, tt := range tests {
		got := ServerConfig{Term: TermConfig{TermTimeout: tt.timeout}}.Sanitize().Term.TermTimeout
		if got != tt.want {
			t.Fatalf("TermTimeout %s: got %s, want %s", tt.timeout, got, tt.want)
		}
	}
}

func TestServerConfigResolveMiddlewares(t *testing.T) {
	r := build(t, nil, nil)
	identity := func(next operation.Handler) operation.Handler { return next }
	cfg := ServerConfig{Middlewares: []MiddlewareFactory{
		func(Resolver) (operation.Middleware, error) { return identity, nil },
	}}

	resolved, err := cfg.ResolveMiddlewares(r)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(resolved.ResolvedMiddlewares()); got != 1 {
		t.Fatalf("resolved %d middlewares, want 1", got)
	}
	if cfg.ResolvedMiddlewares() != nil {
		t.Fatal("ResolveMiddlewares modified its receiver")
	}

	errFactory := errors.New("factory failed")
	cfg.Middlewares = append(cfg.Middlewares, func(Resolver) (operation.Middleware, error) { return nil, errFactory })
	if _, err := cfg.ResolveMiddlewares(r); !errors.Is(err, errFactory) {
		t.Fatalf("got %v, want the factory error", err)
	}
}

func TestTermSig(t *testing.T) {
	ts := &TermSig{}
	if ts.Signaled() {
		t.Fatal("signaled before Signal")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() { _ = ts.Signaled() })
	}
	wg.Go(ts.Signal)
	wg.Go(ts.Signal)
	wg.Wait()
	if !ts.Signaled() {
		t.Fatal("not signaled after Signal")
	}
	select {
	case <-ts.Done():
	default:
		t.Fatal("Done is open after Signal")
	}
}

func TestTermSigDone(t *testing.T) {
	ts := &TermSig{}
	done := ts.Done()
	select {
	case <-done:
		t.Fatal("Done is closed before Signal")
	default:
	}
	ts.Signal()
	<-done
	if ts.Done() != done {
		t.Fatal("Done returned another channel")
	}
}
