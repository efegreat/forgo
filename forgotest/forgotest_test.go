package forgotest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	nethttp "net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/process"
	"github.com/efegreat/forgo/server/cron"
	fgrpc "github.com/efegreat/forgo/server/grpc"
	"github.com/efegreat/forgo/server/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// testClient keeps no connection alive, so that no idle connection outlives a test.
var testClient = &nethttp.Client{Transport: &nethttp.Transport{DisableKeepAlives: true}}

// events records lifecycle steps as "name.step", in order.
type events struct {
	mu    sync.Mutex
	steps []string
}

func (e *events) add(step string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.steps = append(e.steps, step)
}

func (e *events) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.steps)
}

// fakeServer records its lifecycle in events. It is not an IListenerServer, so an env never serves it.
type fakeServer struct {
	name    string
	initErr error
	events  *events
}

func (s *fakeServer) Name() string           { return s.name }
func (s *fakeServer) Term() forgo.TermConfig { return forgo.TermConfig{} }

func (s *fakeServer) Init(ctx context.Context) error {
	s.events.add(s.name + ".init")
	logger.FromCtx(ctx).Info().Msg("init " + s.name)
	return s.initErr
}

func (s *fakeServer) Serve() error {
	s.events.add(s.name + ".serve")
	return nil
}

func (s *fakeServer) Terminate(context.Context) error {
	s.events.add(s.name + ".terminate")
	return nil
}

func (s *fakeServer) Kill() {
	s.events.add(s.name + ".kill")
}

// listenerServer is a fakeServer an env serves: ServeListener serves until Terminate or Kill.
type listenerServer struct {
	*fakeServer
	term     forgo.TermConfig
	serveErr error // returned by ServeListener at once
	termErr  error // returned by Terminate, which stops ServeListener first
	giveUp   bool  // Terminate returns ctx's error once ctx ends, and leaves ServeListener to Kill
	stop     chan struct{}
	stopOnce sync.Once
}

func newListenerServer(name string, ev *events) *listenerServer {
	return &listenerServer{fakeServer: &fakeServer{name: name, events: ev}, stop: make(chan struct{})}
}

func (s *listenerServer) Term() forgo.TermConfig { return s.term }

func (s *listenerServer) ServeListener(ln net.Listener) error {
	defer ln.Close()
	s.events.add(s.name + ".serve")
	if s.serveErr != nil {
		return s.serveErr
	}
	<-s.stop
	return nil
}

func (s *listenerServer) Terminate(ctx context.Context) error {
	s.events.add(s.name + ".terminate")
	if s.giveUp {
		<-ctx.Done()
		return ctx.Err()
	}
	s.stopOnce.Do(func() { close(s.stop) })
	return s.termErr
}

func (s *listenerServer) Kill() {
	s.events.add(s.name + ".kill")
	s.stopOnce.Do(func() { close(s.stop) })
}

// factory is a ServerFactory that returns server, or err.
type factory struct {
	name   string
	server forgo.IServer
	err    error
}

func (f factory) Name() string                              { return f.name }
func (f factory) New(forgo.Resolver) (forgo.IServer, error) { return f.server, f.err }

func factoryOf(s forgo.IServer) factory {
	return factory{name: s.Name(), server: s}
}

// testResource records its Close in events.
type testResource struct {
	name   string
	events *events
}

func (r *testResource) Name() string { return r.name }
func (r *testResource) Value() any   { return r }
func (r *testResource) Close() error { r.events.add(r.name + ".close"); return nil }

type resourceFactory struct{ r *testResource }

func (f resourceFactory) New() (forgo.Resource, error) { return f.r, nil }

// syncBuffer is a concurrency-safe log output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// line returns the first logged line whose field key has value.
func (b *syncBuffer) line(t *testing.T, key, value string) map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for l := range strings.SplitSeq(strings.TrimSpace(b.buf.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		if m[key] == value {
			return m
		}
	}
	t.Fatalf("no log line with %s %q in:\n%s", key, value, b.buf.String())
	return nil
}

// newEnv builds an env of app that logs to out, and fails the test on an error.
func newEnv(t *testing.T, app *forgo.App, out io.Writer) *forgotest.Env {
	t.Helper()
	env, err := forgotest.New(app, forgotest.WithLogger(logger.Config{Output: out}))
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func addr(t *testing.T, env *forgotest.Env, name string) string {
	t.Helper()
	a, err := env.Addr(name)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// closeEnv closes env, and fails the test unless Close returns within 5 seconds.
func closeEnv(t *testing.T, env *forgotest.Env) error {
	t.Helper()
	closed := make(chan error, 1)
	go func() { closed <- env.Close() }()
	select {
	case err := <-closed:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
		return nil
	}
}

func healthRoute() http.RouteConfig {
	return http.RouteConfig{Name: "health", Method: "GET", Path: "/health", Handler: http.HealthHandler()}
}

// The env serves the HTTP server on a loopback listener of its own, through the real chain, and Close neither
// waits for TermSig.Wait nor leaves it unsignaled.
func TestServesHTTP(t *testing.T) {
	out := &syncBuffer{}
	termSig := &forgo.TermSig{Wait: time.Hour}
	env := newEnv(t, forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithServers(http.Config{
			ServerConfig: forgo.ServerConfig{Term: forgo.TermConfig{TermSig: termSig}},
			Addr:         "bogus", // Serve could not listen on it
			Routes:       []http.RouteConfig{healthRoute()},
		}),
	), out)
	url := "http://" + addr(t, env, http.ServerNameHTTP) + "/health"

	resp, err := testClient.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != nethttp.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	l := out.line(t, logger.OperationNameLogKey, "health")
	for _, key := range []string{logger.AppIDLogKey, logger.ServerIDLogKey} {
		if l[key] == nil {
			t.Errorf("no %s in %v", key, l)
		}
	}
	if l[logger.AppNameLogKey] != "shop" || l[logger.ServerNameLogKey] != http.ServerNameHTTP {
		t.Errorf("request line %v", l)
	}

	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}
	if !termSig.Signaled() {
		t.Error("TermSig not signaled")
	}
	if resp, err := testClient.Get(url); err == nil {
		_ = resp.Body.Close()
		t.Fatal("served after Close")
	}
}

func TestServesGRPC(t *testing.T) {
	env := newEnv(t, forgo.NewApp(forgo.WithServers(fgrpc.Config{
		Addr:     "bogus", // Serve could not listen on it
		Services: []fgrpc.ServiceConfig{fgrpc.HealthService()},
	})), io.Discard)

	conn, err := grpc.NewClient(addr(t, env, fgrpc.ServerNameGRPC), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status %v", resp.GetStatus())
	}
	_ = conn.Close()

	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}
}

// Servers that are not IListenerServer servers are created and initialized with the logger fields of App.Run,
// but never served or terminated.
func TestServesListenerServersOnly(t *testing.T) {
	ev := &events{}
	out := &syncBuffer{}
	env := newEnv(t, forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithServers(
			factoryOf(&fakeServer{name: "fake", events: ev}),
			factoryOf(newListenerServer("listener", ev)),
		),
	), out)

	if _, err := env.Addr("fake"); !errors.Is(err, forgotest.ErrNotFound) {
		t.Errorf("Addr of a server that is not served: %v", err)
	}
	l := out.line(t, "message", "init fake")
	for _, key := range []string{logger.AppIDLogKey, logger.ServerIDLogKey} {
		if l[key] == nil {
			t.Errorf("no %s in %v", key, l)
		}
	}
	if l[logger.AppNameLogKey] != "shop" || l[logger.ServerNameLogKey] != "fake" {
		t.Errorf("init line %v", l)
	}

	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}
	for _, step := range ev.list() {
		if strings.HasPrefix(step, "fake.") && step != "fake.init" {
			t.Errorf("unserved server: %s", step)
		}
	}
	if !slices.Contains(ev.list(), "listener.serve") {
		t.Errorf("listener server not served: %v", ev.list())
	}
}

// The HTTP and gRPC servers are IListenerServer servers, and cron is not, so an env never schedules a job.
func TestListenerServers(t *testing.T) {
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer boot.Close()

	for _, tc := range []struct {
		factory  forgo.ServerFactory
		listener bool
	}{
		{http.Config{Routes: []http.RouteConfig{healthRoute()}}, true},
		{fgrpc.Config{Services: []fgrpc.ServiceConfig{fgrpc.HealthService()}}, true},
		{cron.Config{}, false},
	} {
		s, err := tc.factory.New(r)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := s.(forgotest.IListenerServer); ok != tc.listener {
			t.Errorf("%s: IListenerServer %t, want %t", s.Name(), ok, tc.listener)
		}
	}
}

func TestAddr(t *testing.T) {
	env := newEnv(t, forgo.NewApp(forgo.WithServers(
		http.Config{Routes: []http.RouteConfig{healthRoute()}},
		http.Config{Routes: []http.RouteConfig{healthRoute()}},
	)), io.Discard)
	defer env.Close()

	if _, err := env.Addr(http.ServerNameHTTP); !errors.Is(err, forgotest.ErrAmbiguous) {
		t.Errorf("Addr of two HTTP servers: %v", err)
	}
	if _, err := env.Addr("missing"); !errors.Is(err, forgotest.ErrNotFound) {
		t.Errorf("Addr of a missing server: %v", err)
	}
}

func TestNewFailures(t *testing.T) {
	errBoom := errors.New("boom")
	for _, tc := range []struct {
		name string
		app  func(ev *events) *forgo.App
		// closed reports whether the resource was created, and so must be closed.
		closed bool
		want   string
	}{
		{
			name: "nil app",
			app:  func(*events) *forgo.App { return nil },
			want: "forgotest: nil app",
		},
		{
			name: "no servers",
			app:  func(*events) *forgo.App { return forgo.NewApp() },
			want: "forgotest: no servers",
		},
		{
			name: "nil server factory",
			app:  func(*events) *forgo.App { return forgo.NewApp(forgo.WithServers(nil)) },
			want: "forgotest: server 0 is nil",
		},
		{
			name: "bootstrap",
			app: func(ev *events) *forgo.App {
				return forgo.NewApp(
					forgo.WithResources(resourceFactory{&testResource{name: "db", events: ev}}),
					forgo.WithProviders(func() (*strings.Builder, error) { return nil, errBoom }),
					forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: ev})),
				)
			},
			closed: true,
			want:   "boom",
		},
		{
			name: "server new",
			app: func(ev *events) *forgo.App {
				return forgo.NewApp(
					forgo.WithResources(resourceFactory{&testResource{name: "db", events: ev}}),
					forgo.WithServers(factory{name: "broken", err: errBoom}),
				)
			},
			closed: true,
			want:   `forgotest: server "broken": new: boom`,
		},
		{
			name: "server new returns nil",
			app: func(ev *events) *forgo.App {
				return forgo.NewApp(
					forgo.WithResources(resourceFactory{&testResource{name: "db", events: ev}}),
					forgo.WithServers(factory{name: "nil", server: (*fakeServer)(nil)}),
				)
			},
			closed: true,
			want:   `forgotest: server "nil": New returned nil`,
		},
		{
			name: "server init",
			app: func(ev *events) *forgo.App {
				return forgo.NewApp(
					forgo.WithResources(resourceFactory{&testResource{name: "db", events: ev}}),
					forgo.WithServers(factoryOf(&fakeServer{name: "fake", initErr: errBoom, events: ev})),
				)
			},
			closed: true,
			want:   `forgotest: server "fake": init: boom`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := &events{}
			env, err := forgotest.New(tc.app(ev), forgotest.WithLogger(logger.Config{Output: io.Discard}))
			if err == nil || env != nil {
				t.Fatalf("New returned %v, %v", env, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q, want %q", err, tc.want)
			}
			if got := slices.Contains(ev.list(), "db.close"); got != tc.closed {
				t.Errorf("resource closed %t, want %t: %v", got, tc.closed, ev.list())
			}
		})
	}
}

func TestCloseFailures(t *testing.T) {
	errBoom := errors.New("boom")
	for _, tc := range []struct {
		name string
		set  func(s *listenerServer)
		want error // nil for a clean Close
		kill bool
	}{
		{name: "clean", set: func(*listenerServer) {}},
		{name: "serve error", set: func(s *listenerServer) { s.serveErr = errBoom }, want: errBoom},
		{name: "terminate error", set: func(s *listenerServer) { s.termErr = errBoom }, want: errBoom, kill: true},
		{
			name: "terminate timeout",
			set: func(s *listenerServer) {
				s.giveUp = true
				s.term.TermTimeout = 50 * time.Millisecond
			},
			want: context.DeadlineExceeded,
			kill: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := &events{}
			s := newListenerServer("listener", ev)
			tc.set(s)
			env := newEnv(t, forgo.NewApp(
				forgo.WithResources(resourceFactory{&testResource{name: "db", events: ev}}),
				forgo.WithServers(factoryOf(s)),
			), io.Discard)

			err := closeEnv(t, env)
			if (tc.want == nil) != (err == nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("Close: %v, want %v", err, tc.want)
			}
			steps := ev.list()
			if got := slices.Contains(steps, "listener.kill"); got != tc.kill {
				t.Errorf("killed %t, want %t: %v", got, tc.kill, steps)
			}
			// resources are closed after the servers stopped
			if slices.Index(steps, "listener.terminate") > slices.Index(steps, "db.close") {
				t.Errorf("resource closed before the server terminated: %v", steps)
			}
			if err := env.Close(); err != nil {
				t.Errorf("second Close: %v", err)
			}
		})
	}
}

func TestCloseWaitsForGoroutines(t *testing.T) {
	ev := &events{}
	env := newEnv(t, forgo.NewApp(
		forgo.WithResources(resourceFactory{&testResource{name: "db", events: ev}}),
		forgo.WithServers(factoryOf(newListenerServer("listener", ev))),
	), io.Discard)

	release := make(chan struct{})
	process.Go(func() {
		<-release
		ev.add("goroutine.done")
	})
	time.AfterFunc(20*time.Millisecond, func() { close(release) })
	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}
	steps := ev.list()
	if i := slices.Index(steps, "goroutine.done"); i < 0 || i > slices.Index(steps, "db.close") {
		t.Fatalf("resources closed before the goroutine ended: %v", steps)
	}
}

// Without WithLogger, the env logs like the app, but to os.Stderr and to no file.
func TestDefaultLogger(t *testing.T) {
	dir := t.TempDir()
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	orig := os.Stderr
	os.Stderr = stderr
	defer func() { os.Stderr = orig }()

	logFile := filepath.Join(dir, "app.log")
	env, err := forgotest.New(forgo.NewApp(
		forgo.WithLogger(logger.Config{
			Output: io.Discard,
			File:   logger.FileConfig{Enabled: true, Filename: logFile},
		}),
		forgo.WithServers(factoryOf(&fakeServer{name: "fake", events: &events{}})),
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}

	logged, err := os.ReadFile(stderr.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logged, []byte("init fake")) {
		t.Errorf("stderr: %q", logged)
	}
	if _, err := os.Stat(logFile); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("log file written: %v", err)
	}
}
