package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"
)

type fakeBroker struct {
	mu             sync.Mutex
	defaultCreated int
	named          []string
	publishErr     error
	publishBlocks  bool          // Publish waits until its context ends, like a broker blocking publishers
	consumeErr     error         // returned by Consume right away; nil consumes until the server stops
	consuming      chan struct{} // closed once Consume runs, if set
}

func (b *fakeBroker) Name() string { return "fake" }

func (b *fakeBroker) NewDefaultPublisher() (IPublisher, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.defaultCreated++
	return &fakePublisher{err: b.publishErr, blocks: b.publishBlocks}, nil
}

func (b *fakeBroker) NewPublisher(name string) (IPublisher, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.named = append(b.named, name)
	return &fakePublisher{err: b.publishErr, blocks: b.publishBlocks}, nil
}

func (b *fakeBroker) NewSubscriber(string) (ISubscriber, error) {
	return fakeSubscriber{err: b.consumeErr, consuming: b.consuming}, nil
}

type fakePublisher struct {
	err    error
	blocks bool
}

func (p *fakePublisher) Publish(ctx context.Context, _ Message) error {
	if p.blocks {
		<-ctx.Done()
		return ctx.Err()
	}
	return p.err
}
func (p *fakePublisher) Close() error { return nil }

type fakeSubscriber struct {
	err       error
	consuming chan struct{}
}

func (s fakeSubscriber) Consume(ctx context.Context, _ ConsumerState) error {
	if s.consuming != nil {
		close(s.consuming)
	}
	if s.err != nil {
		return s.err
	}
	<-ctx.Done()
	return nil
}
func (fakeSubscriber) Close() error { return nil }

type fakeInbox struct {
	completedCtx context.Context
	status       InboxStatus
	exists       bool
	statusErr    error
	incrementErr error
	failedErr    error
	attempts     int      // returned by IncrementAttempt; zero returns 1
	marked       []string // completed, failed or rejected, in call order
}

func (f *fakeInbox) Status(context.Context, string, string) (InboxStatus, bool, error) {
	return f.status, f.exists, f.statusErr
}
func (f *fakeInbox) IncrementAttempt(context.Context, string, string) (int, error) {
	return max(f.attempts, 1), f.incrementErr
}
func (f *fakeInbox) MarkCompleted(ctx context.Context, _, _ string) error {
	f.completedCtx = ctx
	f.marked = append(f.marked, "completed")
	return nil
}
func (f *fakeInbox) MarkFailed(context.Context, string, string) error {
	f.marked = append(f.marked, "failed")
	return f.failedErr
}
func (f *fakeInbox) MarkRejected(context.Context, string, string) error {
	f.marked = append(f.marked, "rejected")
	return nil
}
func (f *fakeInbox) DeleteBefore(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

type fakeOutbox struct {
	mu         sync.Mutex
	messages   []Message
	claimErrs  int // number of Claim calls that fail before the others succeed
	claims     int
	successful []string
	completed  bool
}

func (f *fakeOutbox) Claim(context.Context, Claim) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claims <= f.claimErrs {
		return nil, errors.New("database down")
	}
	if f.completed {
		return nil, nil
	}
	return f.messages, nil
}
func (f *fakeOutbox) Complete(_ context.Context, _ string, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.successful, f.completed = ids, true
	return nil
}
func (f *fakeOutbox) DeleteProcessedBefore(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

type txKey struct{}

type fakeTransactor struct{}

func (fakeTransactor) Do(ctx context.Context, fn func(context.Context) error) error {
	return fn(context.WithValue(ctx, txKey{}, "tx"))
}

type fakeLocker struct {
	held bool // another delivery holds every lock
	err  error
}

func (l fakeLocker) Lock(context.Context, string, time.Duration) (bool, error) {
	return !l.held && l.err == nil, l.err
}
func (l fakeLocker) WaitLock(ctx context.Context, key string, _ int, _, ttl time.Duration) (bool, error) {
	return l.Lock(ctx, key, ttl)
}
func (fakeLocker) Unlock(context.Context, string) error { return nil }

type fakeDelivery struct {
	msg         Message
	settled     []string // "complete", "retry <attempt>" or "reject", in call order
	ackDeadline bool
	retryErr    error
}

func (d *fakeDelivery) Message() Message { return d.msg }
func (d *fakeDelivery) Complete(ctx context.Context) error {
	_, d.ackDeadline = ctx.Deadline()
	d.settled = append(d.settled, "complete")
	return nil
}
func (d *fakeDelivery) Retry(_ context.Context, attempt int) error {
	d.settled = append(d.settled, fmt.Sprintf("retry %d", attempt))
	return d.retryErr
}
func (d *fakeDelivery) Reject(context.Context) error {
	d.settled = append(d.settled, "reject")
	return nil
}

type event struct {
	ID string `json:"id" validate:"required"`
}

func resolver(t *testing.T, providers ...any) forgo.Resolver {
	t.Helper()
	boot := forgo.NewBootstrap(nil, providers)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	return r
}

func consumerConfig(handler HandlerConfig) ConsumerConfig {
	return ConsumerConfig{Subscribers: []SubscriberConfig{{Name: "sub", Handlers: []HandlerConfig{handler}}}}
}

func noopHandler() Handler[event] {
	return func(context.Context, event) error { return nil }
}

func TestConsumerDependencyValidation(t *testing.T) {
	broker := func() IBroker { return &fakeBroker{} }
	inbox := func() InboxStorage { return &fakeInbox{} }

	for _, tc := range []struct {
		name      string
		handler   HandlerConfig
		providers []any
		wantErr   string
	}{
		{
			name:      "transaction without transactor",
			handler:   HandlerConfig{Name: "evt", Handler: HandlerAs[event](noopHandler)},
			providers: []any{broker, inbox},
			wantErr:   "operation.Transactor",
		},
		{
			name:      "transaction disabled without transactor",
			handler:   HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)},
			providers: []any{broker, inbox},
		},
		{
			name: "lock without locker",
			handler: HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler),
				Lock: operation.Lock{Key: "k", TTL: time.Minute}},
			providers: []any{broker, inbox},
			wantErr:   "operation.ILocker",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := consumerConfig(tc.handler).New(resolver(t, tc.providers...))
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error %v, want it to mention %s", err, tc.wantErr)
			}
		})
	}
}

func TestConsumerTransactionAndAckTimeout(t *testing.T) {
	inbox := &fakeInbox{}
	var handlerCtx context.Context
	newHandler := func() Handler[event] {
		return func(ctx context.Context, _ event) error {
			handlerCtx = ctx
			return nil
		}
	}
	handle := consumerHandler(t, consumerConfig(HandlerConfig{Name: "evt", Handler: HandlerAs[event](newHandler)}),
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return inbox },
		func() operation.Transactor { return fakeTransactor{} },
	)

	headers := types.NewJSONMap[string]()
	d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: headers}}
	handle(context.Background(), d)

	if _, ok := handlerCtx.(*operation.Context); !ok {
		t.Fatalf("handler context is %T, want *operation.Context", handlerCtx)
	}
	if handlerCtx.Value(txKey{}) != "tx" || inbox.completedCtx.Value(txKey{}) != "tx" {
		t.Fatal("handler and inbox did not run on the transaction's context")
	}
	if !slices.Equal(d.settled, []string{"complete"}) || !d.ackDeadline {
		t.Fatalf("settled=%v ackDeadline=%v, want [complete] with a deadline", d.settled, d.ackDeadline)
	}
}

// consumerHandler creates and initializes a consumer for cfg, and returns the function that handles the
// deliveries of its first subscriber.
func consumerHandler(t *testing.T, cfg ConsumerConfig, providers ...any) func(context.Context, IDelivery) {
	t.Helper()
	srv, err := cfg.New(resolver(t, providers...))
	if err != nil {
		t.Fatal(err)
	}
	s := srv.(*ConsumerServer)
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s.subscribers[0].cs.Handler
}

func middlewares(mw operation.Middleware) []forgo.MiddlewareFactory {
	return []forgo.MiddlewareFactory{forgo.MiddlewareAs(func() operation.Middleware { return mw })}
}

func TestConsumerSettlesEveryDelivery(t *testing.T) {
	errDown := errors.New("down")
	failing := middlewares(func(operation.Handler) operation.Handler {
		return func(*operation.Context) error { return errDown }
	})
	var secondErr error
	settleTwice := middlewares(func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			err := next(ctx)
			secondErr = ctx.State.(*State).Delivery.Reject(ctx)
			return err
		}
	})

	for _, tc := range []struct {
		name   string
		inbox  fakeInbox
		locker fakeLocker
		edit   func(*ConsumerConfig)
		want   string
	}{
		{name: "handled", want: "complete"},
		{name: "inbox status error", inbox: fakeInbox{statusErr: errDown}, want: "retry 0"},
		{name: "lock error", locker: fakeLocker{err: errDown}, want: "retry 0", edit: func(c *ConsumerConfig) {
			c.Subscribers[0].Handlers[0].Lock = operation.Lock{Key: "k", TTL: time.Minute}
		}},
		// The holder may have crashed, leaving its lock until it expires: acknowledging would lose the message.
		{name: "lock held by another delivery", locker: fakeLocker{held: true}, want: "retry 0", edit: func(c *ConsumerConfig) {
			c.Subscribers[0].Handlers[0].Lock = operation.Lock{Key: "k", TTL: time.Minute}
		}},
		{name: "server middleware error", want: "retry 0", edit: func(c *ConsumerConfig) { c.Middlewares = failing }},
		{name: "subscriber middleware error", want: "retry 0", edit: func(c *ConsumerConfig) { c.Subscribers[0].Middlewares = failing }},
		{name: "handler middleware error", want: "retry 0", edit: func(c *ConsumerConfig) { c.Subscribers[0].Handlers[0].Middlewares = failing }},
		{name: "base middlewares disabled", want: "retry 0", edit: func(c *ConsumerConfig) {
			c.DisableBaseMiddlewares = true
			c.Middlewares = failing
		}},
		{name: "increment attempt error", inbox: fakeInbox{incrementErr: errDown}, want: "retry 0"},
		{name: "mark failed error", inbox: fakeInbox{attempts: 3, failedErr: errDown}, want: "retry 3", edit: func(c *ConsumerConfig) {
			c.Subscribers[0].Handlers[0].Handler = HandlerAs[event](func() Handler[event] {
				return func(context.Context, event) error { return errDown }
			})
		}},
		{name: "middleware skips next", want: "complete", edit: func(c *ConsumerConfig) {
			c.Subscribers[0].Middlewares = middlewares(func(operation.Handler) operation.Handler {
				return func(*operation.Context) error { return nil }
			})
		}},
		{name: "settled twice", want: "complete", edit: func(c *ConsumerConfig) { c.Subscribers[0].Handlers[0].Middlewares = settleTwice }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ConsumerConfig{Subscribers: []SubscriberConfig{{
				Name:     "sub",
				Handlers: []HandlerConfig{{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)}},
			}}}
			if tc.edit != nil {
				tc.edit(&cfg)
			}
			handle := consumerHandler(t, cfg,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &tc.inbox },
				func() operation.ILocker { return tc.locker },
			)

			d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}}
			handle(context.Background(), d)

			if !slices.Equal(d.settled, []string{tc.want}) {
				t.Fatalf("settled %v, want [%s]", d.settled, tc.want)
			}
		})
	}
	if !errors.Is(secondErr, ErrAlreadySettled) {
		t.Fatalf("second settlement returned %v, want ErrAlreadySettled", secondErr)
	}
}

func TestProducerPublishers(t *testing.T) {
	broker := &fakeBroker{publishErr: errors.New("broker down")}
	outbox := &fakeOutbox{messages: []Message{{ID: "1", Name: "a"}, {ID: "2", Name: "b"}}}
	r := resolver(t,
		func() IBroker { return broker },
		func() OutboxStorage { return outbox },
	)
	cfg := ProducerConfig{
		DefaultPublisher: PublisherConfig{Concurrency: 2},
		Publishers:       []PublisherConfig{{Name: "a"}},
		ErrorPercentage:  50, // no OnErrorPercentage: the breach is logged
	}
	srv, err := cfg.New(r)
	if err != nil {
		t.Fatal(err)
	}
	s := srv.(*ProducerServer)
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if broker.defaultCreated != 2 || len(broker.named) != 1 || broker.named[0] != "a" {
		t.Fatalf("default publishers %d, named %v", broker.defaultCreated, broker.named)
	}

	go s.defaultPublisher.Listen()
	go s.publishers["a"].Listen()
	defer func() {
		s.defaultPublisher.stop()
		s.publishers["a"].stop()
	}()

	count, err := s.processBatch(context.Background())
	if err != nil || count != 0 {
		t.Fatalf("count %d, err %v", count, err)
	}
	if !outbox.completed || len(outbox.successful) != 0 {
		t.Fatalf("completed %v, successful %v", outbox.completed, outbox.successful)
	}
}

func TestNewMessageFromDeliveryHeaderFallbacks(t *testing.T) {
	tests := []struct {
		name          string
		headers       map[string]string
		wantID        string
		wantName      string
		wantCreatedAt time.Time
	}{
		{
			name:          "all headers present",
			headers:       map[string]string{MessageIDHeader: "id-1", MessageNameHeader: "order.placed", MessageTimestampHeader: "1700000000000000000"},
			wantID:        "id-1",
			wantName:      "order.placed",
			wantCreatedAt: time.Unix(0, 1700000000000000000),
		},
		{
			name:          "timestamp missing while name present",
			headers:       map[string]string{MessageIDHeader: "id-2", MessageNameHeader: "order.placed"},
			wantID:        "id-2",
			wantName:      "order.placed",
			wantCreatedAt: time.Unix(0, 0),
		},
		{
			name:          "timestamp unparsable",
			headers:       map[string]string{MessageIDHeader: "id-3", MessageNameHeader: "order.placed", MessageTimestampHeader: "not-a-number"},
			wantID:        "id-3",
			wantName:      "order.placed",
			wantCreatedAt: time.Unix(0, 0),
		},
		{
			name:          "name and timestamp missing",
			headers:       map[string]string{MessageIDHeader: "id-4"},
			wantID:        "id-4",
			wantName:      "unknown",
			wantCreatedAt: time.Unix(0, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMessageFromDelivery("key", "dest", []byte(`{}`), types.NewJSONMapWithData(tt.headers))
			if msg.ID != tt.wantID {
				t.Fatalf("ID = %q, want %q", msg.ID, tt.wantID)
			}
			if msg.Name != tt.wantName {
				t.Fatalf("Name = %q, want %q", msg.Name, tt.wantName)
			}
			if !msg.CreatedAt.Equal(tt.wantCreatedAt) {
				t.Fatalf("CreatedAt = %v, want %v", msg.CreatedAt, tt.wantCreatedAt)
			}
		})
	}
}

func TestNewMessageFromDeliveryGeneratesMissingID(t *testing.T) {
	msg := NewMessageFromDelivery("key", "dest", nil, types.NewJSONMap[string]())
	if msg.ID == "" {
		t.Fatal("expected a generated ID when the header is missing")
	}
}

func TestNewMessageFromDeliveryNilHeaders(t *testing.T) {
	msg := NewMessageFromDelivery("key", "dest", nil, nil)
	if msg.ID == "" || msg.Name != "unknown" || msg.Headers == nil || msg.Headers.Len() != 0 {
		t.Fatalf("got %+v, want a generated ID, the unknown name and empty headers", msg)
	}
}

func TestConsumerProcessing(t *testing.T) {
	errFailed := errors.New("handler failed")
	for _, tc := range []struct {
		name       string
		inbox      fakeInbox
		msgName    string
		payload    string
		handlerErr error
		panics     bool
		want       string   // settlement
		wantMarked []string // inbox marks
		wantRun    bool
	}{
		{name: "handled", want: "complete", wantMarked: []string{"completed"}, wantRun: true},
		{name: "unknown name", msgName: "other", want: "reject"},
		{name: "already completed", inbox: fakeInbox{status: InboxCompleted, exists: true}, want: "complete"},
		{name: "already failed", inbox: fakeInbox{status: InboxFailed, exists: true}, want: "complete"},
		{name: "binding error", payload: `{}`, want: "reject", wantMarked: []string{"rejected"}},
		{name: "error with attempts left", handlerErr: errFailed, want: "retry 1", wantRun: true},
		{name: "error on last attempt", inbox: fakeInbox{attempts: 3}, handlerErr: errFailed, want: "reject", wantMarked: []string{"failed"}, wantRun: true},
		{name: "panic with attempts left", panics: true, want: "retry 1", wantRun: true},
		{name: "panic on last attempt", inbox: fakeInbox{attempts: 3}, panics: true, want: "reject", wantMarked: []string{"failed"}, wantRun: true},
		// An earlier delivery used the last attempt but never settled, for example because it crashed
		// the process: the handler must not run again.
		{name: "attempts exhausted", inbox: fakeInbox{attempts: 4}, want: "reject", wantMarked: []string{"failed"}},
		// The handler knows that retrying cannot help.
		{name: "no retry", handlerErr: fmt.Errorf("order gone: %w", ErrNoRetry), want: "reject", wantMarked: []string{"failed"}, wantRun: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ran := false
			newHandler := func() Handler[event] {
				return func(context.Context, event) error {
					ran = true
					if tc.panics {
						panic("boom")
					}
					return tc.handlerErr
				}
			}
			cfg := ConsumerConfig{Subscribers: []SubscriberConfig{{
				Name:     "sub",
				Handlers: []HandlerConfig{{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](newHandler)}},
			}}}
			srv, err := cfg.New(resolver(t,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &tc.inbox },
			))
			if err != nil {
				t.Fatal(err)
			}
			s := srv.(*ConsumerServer)
			if err := s.Init(context.Background()); err != nil {
				t.Fatal(err)
			}

			msg := Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}
			if tc.msgName != "" {
				msg.Name = tc.msgName
			}
			if tc.payload != "" {
				msg.Payload = []byte(tc.payload)
			}
			d := &fakeDelivery{msg: msg}
			s.subscribers[0].cs.Handler(context.Background(), d)

			if !slices.Equal(d.settled, []string{tc.want}) {
				t.Fatalf("settled %v, want [%s]", d.settled, tc.want)
			}
			if !slices.Equal(tc.inbox.marked, tc.wantMarked) {
				t.Fatalf("inbox marked %v, want %v", tc.inbox.marked, tc.wantMarked)
			}
			if ran != tc.wantRun {
				t.Fatalf("handler ran %v, want %v", ran, tc.wantRun)
			}
		})
	}
}

func TestConsumerServe(t *testing.T) {
	errGaveUp := errors.New("broker gave up reconnecting")
	for _, tc := range []struct {
		name       string
		consumeErr error
	}{
		{name: "stops on terminate"},
		{name: "fails when a subscriber stops consuming", consumeErr: errGaveUp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ConsumerConfig{Subscribers: []SubscriberConfig{{
				Name:     "sub",
				Handlers: []HandlerConfig{{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)}},
			}}}
			broker := &fakeBroker{consumeErr: tc.consumeErr, consuming: make(chan struct{})}
			srv, err := cfg.New(resolver(t,
				func() IBroker { return broker },
				func() InboxStorage { return &fakeInbox{} },
			))
			if err != nil {
				t.Fatal(err)
			}
			if err := srv.Init(context.Background()); err != nil {
				t.Fatal(err)
			}

			served := make(chan error, 1)
			go func() { served <- srv.Serve() }()
			if tc.consumeErr == nil {
				<-broker.consuming
				_ = srv.Terminate(context.Background())
			}

			select {
			case err := <-served:
				if !errors.Is(err, tc.consumeErr) {
					t.Fatalf("Serve returned %v, want %v", err, tc.consumeErr)
				}
				if err != nil && !strings.Contains(err.Error(), "sub") {
					t.Fatalf("error %q does not name the subscriber", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Serve did not return")
			}
			_ = srv.Terminate(context.Background())
		})
	}
}

func TestProducerKeepsRelayingAfterBatchErrors(t *testing.T) {
	outbox := &fakeOutbox{claimErrs: 2, messages: []Message{{ID: "1", Name: "a"}}}
	srv, err := ProducerConfig{Interval: 5 * time.Millisecond}.New(resolver(t,
		func() IBroker { return &fakeBroker{} },
		func() OutboxStorage { return outbox },
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	defer func() { _ = srv.Terminate(context.Background()) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		outbox.mu.Lock()
		completed, successful, claims := outbox.completed, outbox.successful, outbox.claims
		outbox.mu.Unlock()
		if completed {
			if !slices.Equal(successful, []string{"1"}) || claims <= outbox.claimErrs {
				t.Fatalf("successful %v after %d claims", successful, claims)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay stopped after %d claims", claims)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCleanup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxTTL  time.Duration
		batches []int64 // rows deleted by each call, the last one stops the test; -1 fails
	}{
		{name: "disabled", maxTTL: -1},
		// An error waits for the next interval instead of ending the cleanup. Full batches run back to back.
		{name: "retries after an error", maxTTL: time.Hour, batches: []int64{-1, 10, 10, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			del := func(context.Context, time.Time, int) (int64, error) {
				calls++
				if calls >= len(tc.batches) {
					cancel()
				}
				if calls > len(tc.batches) || tc.batches[calls-1] < 0 {
					return 0, errors.New("database down")
				}
				return tc.batches[calls-1], nil
			}

			done := make(chan struct{})
			go func() {
				cleanup(ctx, CleanConfig{MaxTTL: tc.maxTTL, CleanupLimit: 10, CleanupInterval: time.Millisecond}, del)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not stop")
			}
			if calls != len(tc.batches) {
				t.Fatalf("delete called %d times, want %d", calls, len(tc.batches))
			}
		})
	}
}

func TestHandlerTimeout(t *testing.T) {
	ownErr := operation.Error{Code: "own"}
	for _, tc := range []struct {
		name     string
		timeout  time.Duration
		handler  func(ctx context.Context) error
		wantCode string // code of the returned error; empty wants success
		want     string // settlement
	}{
		{name: "no limit", handler: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); ok {
				return errors.New("handler context has a deadline")
			}
			return nil
		}, want: "complete"},
		{name: "finished in time", timeout: time.Hour, handler: func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); !ok {
				return errors.New("handler context has no deadline")
			}
			return nil
		}, want: "complete"},
		{name: "timed out", timeout: 10 * time.Millisecond, handler: func(ctx context.Context) error {
			if err := waitDone(ctx); err != nil {
				return err
			}
			return fmt.Errorf("query: %w", ctx.Err())
		}, wantCode: "msg_timeout", want: "retry 1"},
		{name: "own error after the deadline", timeout: 10 * time.Millisecond, handler: func(ctx context.Context) error {
			if err := waitDone(ctx); err != nil {
				return err
			}
			return ownErr
		}, wantCode: "own", want: "retry 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inbox := &fakeInbox{}
			fn := func(ctx context.Context, _ event) error {
				if _, ok := ctx.(*operation.Context); !ok {
					return fmt.Errorf("handler context is %T, want *operation.Context", ctx)
				}
				return tc.handler(ctx)
			}
			sc := resolvedSubscriber{Name: "sub", MaxAttempt: 3, AckTimeout: time.Second}
			handle := newHandlerWrapper(fn)(defaultBinderFor(t), fakeTransactor{}, inbox, sc, tc.timeout)

			d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}}
			err := handle(&operation.Context{Context: context.Background(), Args: new(types.Map[any, any]), State: newState(d)})

			var opErr operation.Error
			errors.As(err, &opErr)
			if (err == nil) != (tc.wantCode == "") || opErr.Code != tc.wantCode {
				t.Fatalf("error %v (code %q), want code %q", err, opErr.Code, tc.wantCode)
			}
			if !slices.Equal(d.settled, []string{tc.want}) {
				t.Fatalf("settled %v, want [%s]", d.settled, tc.want)
			}
			// The inbox mark and the commit run on the operation's context, not the handler's.
			if tc.wantCode == "" {
				if _, ok := inbox.completedCtx.Deadline(); ok {
					t.Fatal("MarkCompleted ran on the handler's bounded context")
				}
			}
		})
	}
}

// waitDone waits for ctx to end, and fails after 5s instead of hanging the test.
func waitDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("context did not end")
	}
}

func TestProducerBatchEndsBeforeItsClaim(t *testing.T) {
	broker := &fakeBroker{publishBlocks: true}
	outbox := &fakeOutbox{messages: []Message{{ID: "1"}, {ID: "2"}, {ID: "3"}}}
	cfg := ProducerConfig{ClaimTTL: 400 * time.Millisecond, DefaultPublisher: PublisherConfig{Timeout: time.Hour}}
	srv, err := cfg.New(resolver(t,
		func() IBroker { return broker },
		func() OutboxStorage { return outbox },
	))
	if err != nil {
		t.Fatal(err)
	}
	s := srv.(*ProducerServer)
	if err := s.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	go s.defaultPublisher.Listen()
	defer s.defaultPublisher.stop()

	start := time.Now()
	done := make(chan struct{})
	var count int
	go func() {
		count, err = s.processBatch(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Without the batch deadline, three publishes on one blocked publisher take three hours.
		t.Fatal("batch did not end")
	}
	if err != nil || count != 0 {
		t.Fatalf("count %d, err %v", count, err)
	}
	if elapsed := time.Since(start); elapsed >= cfg.ClaimTTL {
		t.Fatalf("batch took %v, want it to end before its claim (%v)", elapsed, cfg.ClaimTTL)
	}
	if !outbox.completed || len(outbox.successful) != 0 {
		t.Fatalf("completed %v, successful %v", outbox.completed, outbox.successful)
	}
}

func TestConfigRejectsNegativeValues(t *testing.T) {
	handler := HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)}
	consumer := func(edit func(*ConsumerConfig)) ConsumerConfig {
		c := ConsumerConfig{Subscribers: []SubscriberConfig{{Name: "sub", Handlers: []HandlerConfig{handler}}}}
		edit(&c)
		return c
	}
	producer := func(edit func(*ProducerConfig)) ProducerConfig {
		c := ProducerConfig{Publishers: []PublisherConfig{{Name: "pub"}}}
		edit(&c)
		return c
	}
	for _, tc := range []struct {
		name    string
		cfg     forgo.ServerFactory
		wantErr string
	}{
		{name: "consumer defaults", cfg: consumer(func(*ConsumerConfig) {})},
		{name: "cleanup disabled", cfg: consumer(func(c *ConsumerConfig) { c.Clean.MaxTTL = -1 })},
		{name: "subscriber concurrency", cfg: consumer(func(c *ConsumerConfig) { c.Subscribers[0].Concurrency = -1 }), wantErr: `subscriber "sub": Concurrency`},
		{name: "max attempt", cfg: consumer(func(c *ConsumerConfig) { c.Subscribers[0].MaxAttempt = -1 }), wantErr: `subscriber "sub": MaxAttempt`},
		{name: "handler timeout", cfg: consumer(func(c *ConsumerConfig) { c.Subscribers[0].Handlers[0].Timeout = -1 }), wantErr: `handler "evt": Timeout`},
		{name: "inbox cleanup interval", cfg: consumer(func(c *ConsumerConfig) { c.Clean.CleanupInterval = -1 }), wantErr: "Clean.CleanupInterval"},
		{name: "producer defaults", cfg: producer(func(*ProducerConfig) {})},
		{name: "limit", cfg: producer(func(c *ProducerConfig) { c.Limit = -1 }), wantErr: "Limit"},
		{name: "interval", cfg: producer(func(c *ProducerConfig) { c.Interval = -1 }), wantErr: "Interval"},
		{name: "claim ttl", cfg: producer(func(c *ProducerConfig) { c.ClaimTTL = -1 }), wantErr: "ClaimTTL"},
		{name: "default publisher concurrency", cfg: producer(func(c *ProducerConfig) { c.DefaultPublisher.Concurrency = -1 }), wantErr: `publisher "default": Concurrency`},
		{name: "publisher timeout", cfg: producer(func(c *ProducerConfig) { c.Publishers[0].Timeout = -1 }), wantErr: `publisher "pub": Timeout`},
		{name: "outbox cleanup limit", cfg: producer(func(c *ProducerConfig) { c.Clean.CleanupLimit = -1 }), wantErr: "Clean.CleanupLimit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.New(resolver(t,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &fakeInbox{} },
				func() OutboxStorage { return &fakeOutbox{} },
			))
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr+" must not be negative")) {
				t.Fatalf("error %v, want one about %s", err, tc.wantErr)
			}
		})
	}
}

type missingService struct{}

func (missingService) Handle(context.Context, event) error { return nil }

func TestConfigValidation(t *testing.T) {
	handler := func(name string) HandlerConfig {
		return HandlerConfig{Name: name, TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)}
	}
	subscriber := func(name string, handlers ...HandlerConfig) SubscriberConfig {
		return SubscriberConfig{Name: name, Handlers: handlers}
	}
	for _, tc := range []struct {
		name    string
		cfg     forgo.ServerFactory
		wantErr string
	}{
		{name: "subscriber without name", cfg: ConsumerConfig{Subscribers: []SubscriberConfig{subscriber("")}},
			wantErr: "worker: subscriber 0 has no name"},
		{name: "duplicate subscriber", cfg: ConsumerConfig{Subscribers: []SubscriberConfig{subscriber("sub"), subscriber("sub")}},
			wantErr: `worker: duplicate subscriber "sub"`},
		{name: "handler without name", cfg: ConsumerConfig{Subscribers: []SubscriberConfig{subscriber("sub", handler(""))}},
			wantErr: `worker: subscriber "sub": handler 0 has no name`},
		{name: "handler without Handler", cfg: ConsumerConfig{Subscribers: []SubscriberConfig{subscriber("sub", HandlerConfig{Name: "evt"})}},
			wantErr: `worker: subscriber "sub": handler "evt" has no Handler`},
		{name: "duplicate handler", cfg: ConsumerConfig{Subscribers: []SubscriberConfig{subscriber("sub", handler("evt"), handler("evt"))}},
			wantErr: `worker: subscriber "sub": duplicate handler "evt"`},
		{name: "handler that cannot be resolved", wantErr: `worker: subscriber "sub": handler "evt": missing dependency`,
			cfg: ConsumerConfig{Subscribers: []SubscriberConfig{subscriber("sub",
				HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerFor(missingService.Handle)})}}},
		{name: "same handler in two subscribers", cfg: ConsumerConfig{Subscribers: []SubscriberConfig{
			subscriber("a", handler("evt")), subscriber("b", handler("evt")),
		}}},
		{name: "publisher without name", cfg: ProducerConfig{Publishers: []PublisherConfig{{}}},
			wantErr: "worker: publisher 0 has no name"},
		{name: "duplicate publisher", cfg: ProducerConfig{Publishers: []PublisherConfig{{Name: "pub"}, {Name: "pub"}}},
			wantErr: `worker: duplicate publisher "pub"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.New(resolver(t,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &fakeInbox{} },
				func() OutboxStorage { return &fakeOutbox{} },
			))
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.HasPrefix(err.Error(), tc.wantErr)) {
				t.Fatalf("error %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// TestSanitizeKeepsCallerConfig checks that Sanitize leaves the subscribers and publishers of the
// caller's config as they are.
func TestSanitizeKeepsCallerConfig(t *testing.T) {
	consumer := consumerConfig(HandlerConfig{Name: "evt", Handler: HandlerAs[event](noopHandler)})
	if got := consumer.Sanitize().Subscribers[0].MaxAttempt; got != 3 {
		t.Fatalf("sanitized MaxAttempt %d, want 3", got)
	}
	if got := consumer.Subscribers[0].MaxAttempt; got != 0 {
		t.Fatalf("caller's MaxAttempt became %d", got)
	}

	producer := ProducerConfig{Publishers: []PublisherConfig{{Name: "pub"}}}
	if got := producer.Sanitize().Publishers[0].Concurrency; got != 1 {
		t.Fatalf("sanitized Concurrency %d, want 1", got)
	}
	if got := producer.Publishers[0].Concurrency; got != 0 {
		t.Fatalf("caller's Concurrency became %d", got)
	}
}
