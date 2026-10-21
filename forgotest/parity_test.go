package forgotest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/forgotest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/server/cron"
	"github.com/efegreat/forgo/server/worker"
)

var errStop = errors.New("stop")

// recordingFactory is a ServerFactory of server that records its New in events.
type recordingFactory struct {
	server forgo.IServer
	events *events
}

func (f recordingFactory) Name() string { return f.server.Name() }

func (f recordingFactory) New(forgo.Resolver) (forgo.IServer, error) {
	f.events.add(f.server.Name() + ".new")
	return f.server, nil
}

// stoppingServer is a fakeServer whose Serve fails once stop returns, which stops forgo.App.Run.
type stoppingServer struct {
	*fakeServer
	stop func()
}

func (s *stoppingServer) Serve() error {
	s.events.add(s.name + ".serve")
	s.stop()
	return errStop
}

// fieldKeys returns the keys of the JSON log line, in order.
func fieldKeys(t *testing.T, line []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(line))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key.(string))
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// rawLine returns the log line of out whose message is msg.
func rawLine(t *testing.T, out *syncBuffer, msg string) []byte {
	t.Helper()
	for line := range strings.SplitSeq(out.String(), "\n") {
		if strings.Contains(line, `"message":"`+msg+`"`) {
			return []byte(line)
		}
	}
	t.Fatalf("no line %q in %s", msg, out.String())
	return nil
}

// forgotest's start repeats the start of forgo.App.Run: the servers are created and initialized in the same
// order, and the context of Init carries the same fields, in the same order. A change to one start that the
// other does not follow fails this test.
func TestStartLikeRun(t *testing.T) {
	// newApp returns the app of two servers, a and b, logging to out, whose b stops Run once it serves.
	newApp := func(ev *events, out *syncBuffer) *forgo.App {
		a := &fakeServer{name: "a", events: ev}
		b := &stoppingServer{fakeServer: &fakeServer{name: "b", events: ev}, stop: func() {}}
		return forgo.NewApp(
			forgo.WithName("shop"),
			forgo.WithLogger(logger.Config{Output: out}),
			forgo.WithServers(recordingFactory{server: a, events: ev}, recordingFactory{server: b, events: ev}),
		)
	}

	runEvents, runOut := &events{}, &syncBuffer{}
	if err := newApp(runEvents, runOut).Run(); !errors.Is(err, errStop) {
		t.Fatalf("Run: %v", err)
	}
	envEvents, envOut := &events{}, &syncBuffer{}
	env, err := forgotest.New(newApp(envEvents, envOut), forgotest.WithLogger(logger.Config{Output: envOut}))
	if err != nil {
		t.Fatal(err)
	}
	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}

	// The steps before serving, which the env never does for these servers.
	started := func(steps []string) []string {
		if i := slices.IndexFunc(steps, func(s string) bool { return strings.HasSuffix(s, ".serve") }); i >= 0 {
			steps = steps[:i]
		}
		return steps
	}
	if run, env := started(runEvents.list()), started(envEvents.list()); !slices.Equal(run, env) {
		t.Errorf("Run started the servers with %v, the env with %v", run, env)
	}

	for _, name := range []string{"a", "b"} {
		runLine, envLine := rawLine(t, runOut, "init "+name), rawLine(t, envOut, "init "+name)
		if run, env := fieldKeys(t, runLine), fieldKeys(t, envLine); !slices.Equal(run, env) {
			t.Errorf("server %s: Run's Init logs %v, the env's %v", name, run, env)
		}
		for _, line := range [][]byte{runLine, envLine} {
			var l map[string]any
			if err := json.Unmarshal(line, &l); err != nil {
				t.Fatal(err)
			}
			if l[logger.AppNameLogKey] != "shop" || l[logger.ServerNameLogKey] != name ||
				l[logger.AppIDLogKey] == "" || l[logger.ServerIDLogKey] == "" {
				t.Errorf("server %s: init line %s", name, line)
			}
		}
	}
}

// probes record what the active servers do when they are served: a cron job runs, the consumer consumes,
// the producer claims from the outbox and publishes.
type probes struct {
	runs      atomic.Int64
	consumed  atomic.Int64
	published atomic.Int64
	box       *memoryOutbox
}

func (p *probes) claims() int {
	p.box.mu.Lock()
	defer p.box.mu.Unlock()
	return len(p.box.claims)
}

// served reports whether every active server did what it does when served.
func (p *probes) served() bool {
	return p.runs.Load() > 0 && p.consumed.Load() > 0 && p.claims() > 0 && p.published.Load() > 0
}

// untouched fails t when any active server did what it does when served.
func (p *probes) untouched(t *testing.T, when string) {
	t.Helper()
	if runs, consumed, claims, published := p.runs.Load(), p.consumed.Load(), p.claims(), p.published.Load(); runs+consumed+int64(claims)+published != 0 {
		t.Errorf("%s: %d job runs, %d consumes, %d claims, %d publishes", when, runs, consumed, claims, published)
	}
}

// probeBroker is a broker resource whose publishers and subscribers record into probes.
type probeBroker struct {
	p *probes
}

func (b probeBroker) New() (forgo.Resource, error) { return probeResource(b), nil }

type probeResource probeBroker

func (r probeResource) Name() string { return "broker" }
func (r probeResource) Value() any   { return probeIBroker(r) }
func (r probeResource) Close() error { return nil }

type probeIBroker probeBroker

func (b probeIBroker) Name() string { return "probe" }
func (b probeIBroker) NewDefaultPublisher() (worker.IPublisher, error) {
	return probePublisher(b), nil
}
func (b probeIBroker) NewPublisher(string) (worker.IPublisher, error) { return probePublisher(b), nil }
func (b probeIBroker) NewSubscriber(string) (worker.ISubscriber, error) {
	return probeSubscriber(b), nil
}

type probePublisher probeBroker

func (p probePublisher) Publish(context.Context, worker.Message) error {
	p.p.published.Add(1)
	return nil
}
func (p probePublisher) Close() error { return nil }

type probeSubscriber probeBroker

func (s probeSubscriber) Consume(ctx context.Context, _ worker.ConsumerState) error {
	s.p.consumed.Add(1)
	<-ctx.Done()
	return nil
}
func (s probeSubscriber) Close() error { return nil }

// activeApp is an app of the built-in active servers, recording into p: cron with a job every second, the
// consumer, and the producer over an outbox that holds a message. extra are added after them.
func activeApp(p *probes, extra ...forgo.ServerFactory) *forgo.App {
	p.box.save(orderMessage("shipped"))
	return forgo.NewApp(
		forgo.WithLogger(logger.Config{Output: &syncBuffer{}}),
		forgo.WithResources(probeBroker{p: p}),
		forgo.WithProviders(append(consumerProviders(&orders{}), func() *memoryOutbox { return p.box })...),
		forgo.WithServers(append([]forgo.ServerFactory{
			cron.Config{Jobs: []cron.JobConfig{{
				Name:     "tick",
				Schedule: "@every 1s",
				Job: cron.As(func() cron.IJob {
					return funcJob(func(context.Context) error {
						p.runs.Add(1)
						return nil
					})
				}),
			}}},
			shippingConsumer(),
			worker.ProducerConfig{Interval: 10 * time.Millisecond},
		}, extra...)...),
	)
}

// The env creates and initializes cron, the consumer and the producer, but never serves them: no job runs on
// its schedule, nothing is consumed, and the relay neither claims nor publishes, so Outbox reads what the
// operations saved. Under forgo.App.Run, the same app does all of it, which shows the probes see serving.
func TestActiveServersNeverServed(t *testing.T) {
	served := &probes{box: newMemoryOutbox()}
	deadline := time.Now().Add(10 * time.Second)
	stopper := &stoppingServer{fakeServer: &fakeServer{name: "stopper", events: &events{}}, stop: func() {
		for !served.served() && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}}
	if err := activeApp(served, factoryOf(stopper)).Run(); !errors.Is(err, errStop) {
		t.Fatalf("Run: %v", err)
	}
	if !served.served() {
		t.Fatalf("under Run: %d job runs, %d consumes, %d claims, %d publishes",
			served.runs.Load(), served.consumed.Load(), served.claims(), served.published.Load())
	}

	p := &probes{box: newMemoryOutbox()}
	env, err := forgotest.New(activeApp(p))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // longer than the job's schedule, and than the relay's interval
	p.untouched(t, "while the env runs")
	if err := closeEnv(t, env); err != nil {
		t.Fatal(err)
	}
	p.untouched(t, "after Close")
}
