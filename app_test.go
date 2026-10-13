package forgo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/process"
)

// fakeServer records its lifecycle in events as "name.step".
type fakeServer struct {
	name     string
	term     TermConfig
	initErr  error
	serveErr error // returned by Serve at once
	termErr  error
	hang     bool // Serve ignores Terminate, and only Kill stops it
	events   *closeLog
	// onServe and onTerminate, when set, run at the start of Serve and Terminate.
	onServe, onTerminate func()

	started  chan struct{}
	stop     chan struct{}
	killed   chan struct{}
	stopOnce sync.Once
	signaled bool // TermSig was signaled when Terminate started
}

func newFakeServer(name string, events *closeLog) *fakeServer {
	return &fakeServer{
		name:    name,
		events:  events,
		started: make(chan struct{}),
		stop:    make(chan struct{}),
		killed:  make(chan struct{}),
	}
}

func (s *fakeServer) Name() string     { return s.name }
func (s *fakeServer) Term() TermConfig { return s.term }

func (s *fakeServer) Init(ctx context.Context) error {
	s.events.add(s.name + ".init")
	logger.FromCtx(ctx).Info().Msg("init " + s.name)
	return s.initErr
}

func (s *fakeServer) Serve() error {
	s.events.add(s.name + ".serve")
	if s.onServe != nil {
		s.onServe()
	}
	close(s.started)
	if s.serveErr != nil {
		return s.serveErr
	}
	if s.hang {
		<-s.killed
	} else {
		<-s.stop
	}
	s.events.add(s.name + ".served")
	return nil
}

func (s *fakeServer) Terminate(context.Context) error {
	s.events.add(s.name + ".terminate")
	if ts := s.term.TermSig; ts != nil {
		s.signaled = ts.Signaled()
	}
	if s.onTerminate != nil {
		s.onTerminate()
	}
	s.stopOnce.Do(func() { close(s.stop) })
	return s.termErr
}

func (s *fakeServer) Kill() {
	s.events.add(s.name + ".kill")
	close(s.killed)
}

// fakeFactory is a ServerFactory that returns server, or err.
type fakeFactory struct {
	name   string
	server IServer
	err    error
}

func (f fakeFactory) Name() string                  { return f.name }
func (f fakeFactory) New(Resolver) (IServer, error) { return f.server, f.err }

func factoryOf(s *fakeServer) fakeFactory {
	return fakeFactory{name: s.name, server: s}
}

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

// lines returns the logged JSON lines.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var lines []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(b.buf.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// line returns the first logged line with message msg.
func (b *syncBuffer) line(t *testing.T, msg string) map[string]any {
	t.Helper()
	for _, l := range b.lines(t) {
		if l["message"] == msg {
			return l
		}
	}
	t.Fatalf("no log line %q", msg)
	return nil
}

func newTestApp(out io.Writer, opts ...AppOption) *App {
	return NewApp(append([]AppOption{WithName("test"), WithLogger(logger.Config{Output: out})}, opts...)...)
}

func indexOf(t *testing.T, events []string, event string) int {
	t.Helper()
	i := slices.Index(events, event)
	if i < 0 {
		t.Fatalf("no event %q in %v", event, events)
	}
	return i
}

func TestAppRunSignal(t *testing.T) {
	events := &closeLog{}
	out := &syncBuffer{}
	a, b := newFakeServer("a", events), newFakeServer("b", events)
	a.term = TermConfig{TermSig: &TermSig{Wait: 20 * time.Millisecond}, TermTimeout: time.Second}
	a.onServe = func() {
		process.Go(func() {
			<-a.stop
			time.Sleep(10 * time.Millisecond)
			events.add("goroutine.done")
		})
	}
	resource := &testResource{name: "db", value: &testDB{}, log: events}

	done := make(chan error, 1)
	go func() {
		done <- newTestApp(out,
			WithResources(resourceOf(resource)),
			WithServers(factoryOf(a), factoryOf(b)),
		).Run()
	}()
	<-a.started
	<-b.started
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	got := events.get()
	if indexOf(t, got, "a.init") > indexOf(t, got, "b.init") || indexOf(t, got, "b.init") > indexOf(t, got, "a.serve") {
		t.Fatalf("servers not initialized in order before serving: %v", got)
	}
	for _, step := range []string{"a.served", "b.served", "goroutine.done"} {
		if indexOf(t, got, step) > indexOf(t, got, "db") {
			t.Fatalf("resource closed before %s: %v", step, got)
		}
	}
	if !a.signaled {
		t.Fatal("TermSig not signaled before Terminate")
	}

	if l := out.line(t, "init a"); l[logger.ServerNameLogKey] != "a" || l[logger.ServerIDLogKey] == nil ||
		l[logger.AppNameLogKey] != "test" || l[logger.AppIDLogKey] == nil {
		t.Fatalf("Init context logger lacks fields: %v", l)
	}
	out.line(t, "app terminating: terminated signal received")
}

func TestAppRunServeError(t *testing.T) {
	events := &closeLog{}
	out := &syncBuffer{}
	errListen := errors.New("listen: address in use")
	failing, other := newFakeServer("failing", events), newFakeServer("other", events)
	failing.serveErr = errListen

	err := newTestApp(out, WithServers(factoryOf(failing), factoryOf(other))).Run()
	if !errors.Is(err, errListen) || err.Error() != `app: server "failing": serve: listen: address in use` {
		t.Fatalf("got %v, want the serve error", err)
	}
	got := events.get()
	indexOf(t, got, "other.terminate")
	indexOf(t, got, "other.served")

	l := out.line(t, "serve error")
	if l[logger.ServerNameLogKey] != "failing" || l["level"] != "error" || l[logger.ErrorSeverityLogKey] != float64(2) {
		t.Fatalf("serve error line: %v", l)
	}
	out.line(t, "app terminating after a server failed")
}

func TestAppRunTermination(t *testing.T) {
	errServe := errors.New("serve failed")
	errTerm := errors.New("terminate failed")
	tests := []struct {
		name   string
		setup  func(s *fakeServer)
		is     error
		want   string
		killed bool
	}{
		{
			name:  "zero TermTimeout means the default",
			setup: func(*fakeServer) {},
		},
		{
			name:  "terminate error",
			setup: func(s *fakeServer) { s.termErr = errTerm },
			is:    errTerm,
			want:  `app: server "s": terminate: terminate failed`,
		},
		{
			name: "Serve does not return in time",
			setup: func(s *fakeServer) {
				s.hang = true
				s.term.TermTimeout = 20 * time.Millisecond
			},
			is:     context.DeadlineExceeded,
			want:   `app: server "s": terminate: context deadline exceeded`,
			killed: true,
		},
		{
			name: "Terminate does not return in time",
			setup: func(s *fakeServer) {
				s.term.TermTimeout = 20 * time.Millisecond
				s.onTerminate = func() { time.Sleep(time.Second) }
			},
			is:     context.DeadlineExceeded,
			killed: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &closeLog{}
			out := &syncBuffer{}
			s, trigger := newFakeServer("s", events), newFakeServer("trigger", events)
			trigger.serveErr = errServe
			tt.setup(s)
			trigger.onServe = func() { <-s.started }

			err := newTestApp(out, WithServers(factoryOf(s), factoryOf(trigger))).Run()
			if !errors.Is(err, errServe) {
				t.Fatalf("got %v, want the serve error", err)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("got %v, want %v", err, tt.is)
			}
			if tt.is == nil && strings.Contains(err.Error(), "terminate") {
				t.Fatalf("got %v, want no termination error", err)
			}
			if tt.want != "" && !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want %q", err, tt.want)
			}
			if killed := slices.Contains(events.get(), "s.kill"); killed != tt.killed {
				t.Fatalf("killed: %v, want %v (%v)", killed, tt.killed, events.get())
			}
			if tt.killed {
				if l := out.line(t, "server termination timeout, killing"); l["level"] != "error" || l[logger.ErrorSeverityLogKey] != float64(1) {
					t.Fatalf("kill line: %v", l)
				}
			}
		})
	}
}

func TestAppRunStartFailures(t *testing.T) {
	errNew := errors.New("new failed")
	errInit := errors.New("init failed")
	errProvider := errors.New("provider failed")
	tests := []struct {
		name      string
		servers   func(events *closeLog) []ServerFactory
		providers []any
		want      string
		is        error
		opened    bool // whether the resource was opened, and so closed
	}{
		{
			name:    "no servers",
			servers: func(*closeLog) []ServerFactory { return nil },
			is:      errNoServers,
		},
		{
			name:    "nil server",
			servers: func(*closeLog) []ServerFactory { return []ServerFactory{nil} },
			want:    "app: server 0 is nil",
		},
		{
			name: "bootstrap",
			servers: func(events *closeLog) []ServerFactory {
				return []ServerFactory{factoryOf(newFakeServer("a", events))}
			},
			providers: []any{func() (*service, error) { return nil, errProvider }},
			is:        errProvider,
			opened:    true,
		},
		{
			name: "New fails",
			servers: func(events *closeLog) []ServerFactory {
				return []ServerFactory{factoryOf(newFakeServer("a", events)), fakeFactory{name: "broken", err: errNew}}
			},
			want:   `app: server "broken": new: new failed`,
			is:     errNew,
			opened: true,
		},
		{
			name: "New returns nil",
			servers: func(*closeLog) []ServerFactory {
				return []ServerFactory{fakeFactory{name: "broken", server: (*fakeServer)(nil)}}
			},
			want:   `app: server "broken": New returned nil`,
			opened: true,
		},
		{
			name: "Init fails",
			servers: func(events *closeLog) []ServerFactory {
				s := newFakeServer("a", events)
				s.initErr = errInit
				return []ServerFactory{factoryOf(s)}
			},
			want:   `app: server "a": init: init failed`,
			is:     errInit,
			opened: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := &closeLog{}
			out := &syncBuffer{}
			opened := false
			resource := resourceFunc(func() (Resource, error) {
				opened = true
				return &testResource{name: "db", value: &testDB{}, log: events}, nil
			})

			err := newTestApp(out,
				WithResources(resource),
				WithProviders(tt.providers...),
				WithServers(tt.servers(events)...),
			).Run()
			if err == nil {
				t.Fatal("Run succeeded")
			}
			if tt.want != "" && err.Error() != tt.want {
				t.Fatalf("got %q, want %q", err, tt.want)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("got %v, want %v", err, tt.is)
			}
			if opened != tt.opened || slices.Contains(events.get(), "db") != tt.opened {
				t.Fatalf("opened %v, events %v, want opened and closed: %v", opened, events.get(), tt.opened)
			}
			for _, e := range events.get() {
				if strings.HasSuffix(e, ".serve") {
					t.Fatalf("a server served: %v", events.get())
				}
			}

			lines := out.lines(t)
			last := lines[len(lines)-1]
			if last["level"] != "error" || last[logger.ErrorSeverityLogKey] != float64(2) || last["error"] != err.Error() {
				t.Fatalf("failure line: %v", last)
			}
		})
	}
}

func TestAppRunCloseError(t *testing.T) {
	errServe := errors.New("serve failed")
	errClose := errors.New("close failed")
	s := newFakeServer("s", &closeLog{})
	s.serveErr = errServe
	out := &syncBuffer{}

	err := newTestApp(out,
		WithResources(resourceOf(&testResource{name: "db", value: 1, closeErr: errClose})),
		WithServers(factoryOf(s)),
	).Run()
	if !errors.Is(err, errServe) || !errors.Is(err, errClose) {
		t.Fatalf("got %v, want both errors", err)
	}
	if l := out.line(t, "closing resources failed"); l[logger.ErrorSeverityLogKey] != float64(1) {
		t.Fatalf("close line: %v", l)
	}
}

func TestNewApp(t *testing.T) {
	if got := NewApp().name; got != defaultAppName {
		t.Fatalf("name %q, want %q", got, defaultAppName)
	}
	a := NewApp(
		WithName("shop"),
		WithResources(resourceOf(&testResource{name: "a"})),
		WithResources(resourceOf(&testResource{name: "b"})),
		WithProviders(newRepository),
		WithProviders(newService),
		WithServers(fakeFactory{name: "x"}, fakeFactory{name: "y"}),
	)
	if a.name != "shop" || len(a.resources) != 2 || len(a.providers) != 2 || len(a.servers) != 2 {
		t.Fatalf("options not applied: %+v", a)
	}
}

// TestAppRunSecondSignal runs an app in a subprocess whose server never stops, and checks that a second
// SIGTERM during termination terminates the process.
func TestAppRunSecondSignal(t *testing.T) {
	if os.Getenv("FORGO_TEST_SECOND_SIGNAL") == "1" {
		s := newFakeServer("hang", &closeLog{})
		s.hang = true
		s.term.TermTimeout = time.Minute
		s.onServe = func() { fmt.Println("serving") }
		s.onTerminate = func() { fmt.Println("terminating") }
		_ = newTestApp(io.Discard, WithServers(factoryOf(s))).Run()
		fmt.Println("returned")
		os.Exit(0)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAppRunSecondSignal$")
	cmd.Env = append(os.Environ(), "FORGO_TEST_SECOND_SIGNAL=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(stdout)
	waitFor := func(want string) {
		t.Helper()
		for lines.Scan() {
			if lines.Text() == want {
				return
			}
		}
		t.Fatalf("subprocess never printed %q", want)
	}

	waitFor("serving")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitFor("terminating")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	err = cmd.Wait()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("got %v, want the process killed by the signal", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Fatalf("got %v, want the process killed by SIGTERM", err)
	}
}
