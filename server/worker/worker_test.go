package worker

import (
	"context"
	"errors"
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
}

func (b *fakeBroker) Name() string { return "fake" }

func (b *fakeBroker) NewDefaultPublisher() (IPublisher, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.defaultCreated++
	return &fakePublisher{err: b.publishErr}, nil
}

func (b *fakeBroker) NewPublisher(name string) (IPublisher, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.named = append(b.named, name)
	return &fakePublisher{err: b.publishErr}, nil
}

func (b *fakeBroker) NewSubscriber(string) (ISubscriber, error) { return fakeSubscriber{}, nil }

type fakePublisher struct{ err error }

func (p *fakePublisher) Publish(context.Context, Message) error { return p.err }
func (p *fakePublisher) Close() error                           { return nil }

type fakeSubscriber struct{}

func (fakeSubscriber) Consume(ctx context.Context, _ ConsumerState) error { <-ctx.Done(); return nil }
func (fakeSubscriber) Close() error                                       { return nil }

type fakeInbox struct {
	completedCtx context.Context
}

func (f *fakeInbox) Status(context.Context, string, string) (InboxStatus, bool, error) {
	return InboxWaiting, false, nil
}
func (f *fakeInbox) IncrementAttempt(context.Context, string, string) (int, error) { return 1, nil }
func (f *fakeInbox) MarkCompleted(ctx context.Context, _, _ string) error {
	f.completedCtx = ctx
	return nil
}
func (f *fakeInbox) MarkFailed(context.Context, string, string) error   { return nil }
func (f *fakeInbox) MarkRejected(context.Context, string, string) error { return nil }
func (f *fakeInbox) DeleteBefore(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

type fakeOutbox struct {
	messages   []Message
	successful []string
	completed  bool
}

func (f *fakeOutbox) Claim(context.Context, Claim) ([]Message, error) { return f.messages, nil }
func (f *fakeOutbox) Complete(_ context.Context, _ string, ids []string) error {
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

type fakeDelivery struct {
	msg         Message
	completed   bool
	ackDeadline bool
}

func (d *fakeDelivery) Message() Message { return d.msg }
func (d *fakeDelivery) Complete(ctx context.Context) error {
	_, d.ackDeadline = ctx.Deadline()
	d.completed = true
	return nil
}
func (d *fakeDelivery) Retry(context.Context) error  { return nil }
func (d *fakeDelivery) Reject(context.Context) error { return nil }

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
				Lock: operation.Lock{Key: "k", Ttl: time.Minute}},
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
	r := resolver(t,
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return inbox },
		func() operation.Transactor { return fakeTransactor{} },
	)
	srv, err := consumerConfig(HandlerConfig{Name: "evt", Handler: HandlerAs[event](newHandler)}).New(r)
	if err != nil {
		t.Fatal(err)
	}
	s := srv.(*ConsumerServer)
	sb := s.cfg.resolvedSubscribers[0]
	handle := s.createHandler(sb.Handlers[0], sb)

	headers := types.NewJSONMap[string]()
	d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: headers}}
	handle(context.Background(), d)

	if _, ok := handlerCtx.(*operation.Context); !ok {
		t.Fatalf("handler context is %T, want *operation.Context", handlerCtx)
	}
	if handlerCtx.Value(txKey{}) != "tx" || inbox.completedCtx.Value(txKey{}) != "tx" {
		t.Fatal("handler and inbox did not run on the transaction's context")
	}
	if !d.completed || !d.ackDeadline {
		t.Fatalf("completed=%v ackDeadline=%v, want both", d.completed, d.ackDeadline)
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
