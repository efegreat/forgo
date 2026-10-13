package worker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efegreat/forgo/types"
)

// subscriberBroker is a fakeBroker whose subscribers are sub.
type subscriberBroker struct {
	fakeBroker
	sub ISubscriber
}

func (b *subscriberBroker) NewSubscriber(string) (ISubscriber, error) { return b.sub, nil }

// recordingSubscriber consumes until its context ends, and counts its Consume and Close calls.
type recordingSubscriber struct {
	consumed, closed *atomic.Int32
	stops            bool // Consume returns nil at once, as if it stopped on its own
}

func (s recordingSubscriber) Consume(ctx context.Context, _ ConsumerState) error {
	s.consumed.Add(1)
	if !s.stops {
		<-ctx.Done()
	}
	return nil
}
func (s recordingSubscriber) Close() error {
	s.closed.Add(1)
	return nil
}

// onceSubscriber hands d to the consumer, then consumes until its context ends and waits for the handler,
// as broker adapters do.
type onceSubscriber struct{ d IDelivery }

func (s onceSubscriber) Consume(ctx context.Context, cs ConsumerState) error {
	var wg sync.WaitGroup
	wg.Go(func() { cs.Handler(ctx, s.d) })
	<-ctx.Done()
	wg.Wait()
	return nil
}
func (onceSubscriber) Close() error { return nil }

// gatedBroker is a fakeBroker whose default publisher is pub.
type gatedBroker struct {
	fakeBroker
	pub *gatedPublisher
}

func (b *gatedBroker) NewDefaultPublisher() (IPublisher, error) { return b.pub, nil }

// gatedPublisher reports every publish on started, then publishes once release is closed, or fails when
// its context ends first. It counts its Close calls.
type gatedPublisher struct {
	started chan struct{}
	release chan struct{}
	closed  atomic.Int32
}

func (p *gatedPublisher) Publish(ctx context.Context, _ Message) error {
	p.started <- struct{}{}
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *gatedPublisher) Close() error {
	p.closed.Add(1)
	return nil
}

// serve runs srv's Serve, and returns the channel its error arrives on.
func serve(srv interface{ Serve() error }) <-chan error {
	served := make(chan error, 1)
	go func() { served <- srv.Serve() }()
	return served
}

// receive returns what ch receives, and fails the test after 5s instead of hanging.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen", what)
		var zero T
		return zero
	}
}

// TestTerminateBeforeServe checks that a Serve that starts after Terminate, as when the app stops right
// after starting, returns at once without consuming or relaying, and closes what Init created.
func TestTerminateBeforeServe(t *testing.T) {
	t.Run("consumer", func(t *testing.T) {
		var consumed, closed atomic.Int32
		cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)})
		srv, err := cfg.New(resolver(t,
			func() IBroker {
				return &subscriberBroker{sub: recordingSubscriber{consumed: &consumed, closed: &closed}}
			},
			func() InboxStorage { return &fakeInbox{} },
		))
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := srv.Terminate(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := receive(t, serve(srv), "Serve returning"); err != nil {
			t.Fatalf("Serve returned %v", err)
		}
		if consumed.Load() != 0 || closed.Load() != 1 {
			t.Fatalf("consumed %d times, closed %d times, want 0 and 1", consumed.Load(), closed.Load())
		}
	})

	t.Run("producer", func(t *testing.T) {
		pub := &gatedPublisher{started: make(chan struct{}, 1), release: make(chan struct{})}
		outbox := &fakeOutbox{messages: []Message{{ID: "1"}}}
		srv, err := ProducerConfig{}.New(resolver(t,
			func() IBroker { return &gatedBroker{pub: pub} },
			func() OutboxStorage { return outbox },
		))
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.Init(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := srv.Terminate(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := receive(t, serve(srv), "Serve returning"); err != nil {
			t.Fatalf("Serve returned %v", err)
		}
		outbox.mu.Lock()
		claims := outbox.claims
		outbox.mu.Unlock()
		if claims != 0 || pub.closed.Load() != 1 {
			t.Fatalf("claimed %d times, closed %d times, want 0 and 1", claims, pub.closed.Load())
		}
	})
}

// TestConsumerTerminateAndKill checks that Terminate leaves a message in flight running until its context
// ends, and that Kill then cancels the handler's context.
func TestConsumerTerminateAndKill(t *testing.T) {
	started := make(chan struct{})
	newHandler := func() Handler[event] {
		return func(ctx context.Context, _ event) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
	}
	d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}}
	cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](newHandler)})
	srv, err := cfg.New(resolver(t,
		func() IBroker { return &subscriberBroker{sub: onceSubscriber{d: d}} },
		func() InboxStorage { return &fakeInbox{} },
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	served := serve(srv)
	receive(t, started, "handling")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := srv.Terminate(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Terminate returned %v, want it to give up when its context ends", err)
	}
	if err := receive(t, served, "Serve returning"); err != nil {
		t.Fatalf("Serve returned %v", err)
	}

	srv.Kill()
	if err := srv.Terminate(context.Background()); err != nil {
		t.Fatalf("Terminate after Kill returned %v", err)
	}
	// The handler failed on its canceled context, and the message is retried.
	if !slices.Equal(d.settled, []string{"retry 1"}) {
		t.Fatalf("settled %v, want [retry 1]", d.settled)
	}
}

// TestProducerTerminateAndKill checks that Terminate lets the batch in flight publish, and that Kill
// cancels its publishes instead.
func TestProducerTerminateAndKill(t *testing.T) {
	for _, tc := range []struct {
		name string
		kill bool
		want []string // published IDs
	}{
		{name: "terminate", want: []string{"1"}},
		{name: "kill", kill: true, want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pub := &gatedPublisher{started: make(chan struct{}, 1), release: make(chan struct{})}
			outbox := &fakeOutbox{messages: []Message{{ID: "1"}}}
			srv, err := ProducerConfig{}.New(resolver(t,
				func() IBroker { return &gatedBroker{pub: pub} },
				func() OutboxStorage { return outbox },
			))
			if err != nil {
				t.Fatal(err)
			}
			if err := srv.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			served := serve(srv)
			receive(t, pub.started, "publishing")

			terminated := make(chan error, 1)
			go func() { terminated <- srv.Terminate(context.Background()) }()
			if err := receive(t, served, "Serve returning"); err != nil {
				t.Fatalf("Serve returned %v", err)
			}
			if tc.kill {
				srv.Kill()
			} else {
				close(pub.release)
			}
			if err := receive(t, terminated, "Terminate returning"); err != nil {
				t.Fatalf("Terminate returned %v", err)
			}

			outbox.mu.Lock()
			defer outbox.mu.Unlock()
			if !outbox.completed || !slices.Equal(outbox.successful, tc.want) {
				t.Fatalf("completed %v, published %v, want %v", outbox.completed, outbox.successful, tc.want)
			}
		})
	}
}

// TestConsumerFailsWhenASubscriberStops checks that a subscriber whose Consume returns nil on its own
// stops the app like one that fails.
func TestConsumerFailsWhenASubscriberStops(t *testing.T) {
	var consumed, closed atomic.Int32
	cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)})
	srv, err := cfg.New(resolver(t,
		func() IBroker {
			return &subscriberBroker{sub: recordingSubscriber{consumed: &consumed, closed: &closed, stops: true}}
		},
		func() InboxStorage { return &fakeInbox{} },
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = receive(t, serve(srv), "Serve returning")
	if !errors.Is(err, errStoppedConsuming) || !strings.Contains(err.Error(), "sub") {
		t.Fatalf("Serve returned %v, want errStoppedConsuming naming the subscriber", err)
	}
	if err := srv.Terminate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("closed %d times, want 1", closed.Load())
	}
}
