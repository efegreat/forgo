package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/efegreat/forgo/internal/observertest"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

// TestOperationObserver checks that the observer sees every handled message once, including the redeliveries
// the inbox settles and the panics Recovery turns into errors.
func TestOperationObserver(t *testing.T) {
	errFailed := operation.Error{Code: "failed", Status: operation.StatusUnavailable}
	for _, tc := range []struct {
		name       string
		inbox      fakeInbox
		msgName    string
		payload    string
		handlerErr error
		panics     bool
		wantName   string
		wantCode   string
	}{
		{name: "handled", wantName: "evt"},
		{name: "handler error", handlerErr: errFailed, wantName: "evt", wantCode: "failed"},
		{name: "panic", panics: true, wantName: "evt", wantCode: "x"},
		{name: "binding error", payload: `{}`, wantName: "evt", wantCode: errBind.Code},
		{name: "already completed", inbox: fakeInbox{status: InboxCompleted, exists: true}, wantName: "evt"},
		{name: "unknown name", msgName: "other", wantName: notFoundName, wantCode: errNoHandler.Code},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &observertest.Observer{}
			newHandler := func() Handler[event] {
				return func(context.Context, event) error {
					if tc.panics {
						panic("boom")
					}
					return tc.handlerErr
				}
			}
			cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](newHandler)})
			handle := consumerHandler(t, cfg,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &tc.inbox },
				obs.Provider(),
			)

			msg := Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}
			if tc.msgName != "" {
				msg.Name = tc.msgName
			}
			if tc.payload != "" {
				msg.Payload = []byte(tc.payload)
			}
			handle(context.Background(), &fakeDelivery{msg: msg})

			e := obs.Last(t, 1)
			if e.Type != OperationTypeMessageHandler || e.Name != tc.wantName || e.Error.Code != tc.wantCode {
				t.Fatalf("observed %s %q with code %q, want %q with %q", e.Type, e.Name, e.Error.Code, tc.wantName, tc.wantCode)
			}
		})
	}
}

// A delivery whose lock another delivery holds is retried without running, so it is not observed, like it
// is not logged.
func TestOperationObserverSkipsLockedDeliveries(t *testing.T) {
	obs := &observertest.Observer{}
	cfg := consumerConfig(HandlerConfig{
		Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler),
		Lock: operation.Lock{Key: "k", TTL: time.Minute},
	})
	handle := consumerHandler(t, cfg,
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return &fakeInbox{} },
		func() operation.ILocker { return fakeLocker{held: true} },
		obs.Provider(),
	)
	d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`)}}
	handle(context.Background(), d)

	if len(d.settled) != 1 || obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatalf("settled %v, observed %d operations, want a retry and none", d.settled, len(obs.Ends()))
	}
}

func TestOperationObserverWithoutBaseMiddlewares(t *testing.T) {
	obs := &observertest.Observer{}
	cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)})
	cfg.DisableBaseMiddlewares = true
	handle := consumerHandler(t, cfg,
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return &fakeInbox{} },
		obs.Provider(),
	)
	handle(context.Background(), &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`)}})

	if obs.Starts() != 0 || len(obs.Ends()) != 0 {
		t.Fatal("observed a message without base middlewares")
	}
}

// TestOperationObserverAllocations checks that an observer adds no allocation to a handled message, nor to a
// failed one.
func TestOperationObserverAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	errFailed := operation.Error{Code: "failed", Status: operation.StatusUnavailable}
	allocs := func(handlerErr error, providers ...any) float64 {
		cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](func() Handler[event] {
			return func(context.Context, event) error { return handlerErr }
		})})
		handle := consumerHandler(t, cfg, append([]any{
			func() IBroker { return &fakeBroker{} },
			func() InboxStorage { return &countingInbox{} },
		}, providers...)...)
		msg := Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}
		ctx := logger.NewCtx(context.Background(), zerolog.New(io.Discard))
		return testing.AllocsPerRun(100, func() { handle(ctx, &nopDelivery{msg: msg}) })
	}
	for _, err := range []error{nil, errFailed} {
		without := allocs(err)
		if with := allocs(err, func() operation.IObserver { return nopObserver{} }); with != without {
			t.Errorf("handler returning %v: %v allocations with an observer, %v without", err, with, without)
		}
	}
}

// countingInbox is an InboxStorage that allocates nothing, for allocation tests.
type countingInbox struct{ fakeInbox }

func (*countingInbox) MarkCompleted(context.Context, string, string) error { return nil }
func (*countingInbox) MarkFailed(context.Context, string, string) error    { return nil }

// nopDelivery is an IDelivery that allocates nothing, for allocation tests.
type nopDelivery struct{ msg Message }

func (d *nopDelivery) Message() Message               { return d.msg }
func (*nopDelivery) Complete(context.Context) error   { return nil }
func (*nopDelivery) Retry(context.Context, int) error { return nil }
func (*nopDelivery) Reject(context.Context) error     { return nil }

type nopObserver struct{}

func (nopObserver) Start(*operation.Context)                      {}
func (nopObserver) End(*operation.Context, operation.Observation) {}

// signalObserver records the settlements, relay batches and locks it is told about.
type signalObserver struct {
	observertest.LockObserver

	mu          sync.Mutex
	settlements []settlement
	batches     []RelayBatch
}

// settlement is what signalObserver recorded of a call of Settled.
type settlement struct {
	Name, Subscriber string
	Settlement
}

func (o *signalObserver) Provider() func() operation.IObserver {
	return func() operation.IObserver { return o }
}

func (o *signalObserver) Settled(ctx *operation.Context, s Settlement) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.settlements = append(o.settlements, settlement{Name: ctx.Name, Subscriber: ctx.State.(*State).Subscriber, Settlement: s})
}

func (o *signalObserver) Relayed(_ context.Context, b RelayBatch) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.batches = append(o.batches, b)
}

func (o *signalObserver) Settlements() []settlement {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]settlement(nil), o.settlements...)
}

func (o *signalObserver) Batches() []RelayBatch {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]RelayBatch(nil), o.batches...)
}

// TestDeliveryObserver checks that every delivery is reported settled exactly once, however it is settled.
func TestDeliveryObserver(t *testing.T) {
	errDown := errors.New("down")
	errFailed := errors.New("handler failed")
	errBroker := errors.New("channel closed")
	failing := middlewares(func(operation.Handler) operation.Handler {
		return func(*operation.Context) error { return errDown }
	})

	for _, tc := range []struct {
		name       string
		inbox      fakeInbox
		payload    string
		msgName    string
		handlerErr error
		retryErr   error
		edit       func(*ConsumerConfig)
		want       Settlement
		wantName   string
	}{
		{name: "acknowledged", want: Settlement{Kind: SettlementCompleted, Attempt: 1}},
		{name: "retried", handlerErr: errFailed, want: Settlement{Kind: SettlementRetried, Attempt: 1}},
		{name: "rejected after the last attempt", inbox: fakeInbox{attempts: 3}, handlerErr: errFailed,
			want: Settlement{Kind: SettlementRejected, Attempt: 3}},
		{name: "rejected for its payload", payload: `{}`, want: Settlement{Kind: SettlementRejected, Attempt: 1}},
		{name: "rejected without a handler", msgName: "other", want: Settlement{Kind: SettlementRejected}, wantName: notFoundName},
		{name: "completed duplicate", inbox: fakeInbox{status: InboxCompleted, exists: true},
			want: Settlement{Kind: SettlementCompleted, Duplicate: true}},
		{name: "failed duplicate", inbox: fakeInbox{status: InboxFailed, exists: true},
			want: Settlement{Kind: SettlementRejected, Duplicate: true}},
		{name: "unsettled error", inbox: fakeInbox{statusErr: errDown}, want: Settlement{Kind: SettlementRetried}},
		{name: "unsettled success", want: Settlement{Kind: SettlementCompleted}, edit: func(c *ConsumerConfig) {
			c.Subscribers[0].Middlewares = middlewares(func(operation.Handler) operation.Handler {
				return func(*operation.Context) error { return nil }
			})
		}},
		{name: "broker failure", handlerErr: errFailed, retryErr: errBroker,
			want: Settlement{Kind: SettlementRetried, Attempt: 1, Err: errBroker}},
		{name: "base middlewares disabled", want: Settlement{Kind: SettlementRetried}, edit: func(c *ConsumerConfig) {
			c.DisableBaseMiddlewares = true
			c.Middlewares = failing
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &signalObserver{}
			newHandler := func() Handler[event] {
				return func(context.Context, event) error { return tc.handlerErr }
			}
			cfg := ConsumerConfig{Subscribers: []SubscriberConfig{{
				Name:     "sub",
				Handlers: []HandlerConfig{{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](newHandler)}},
			}}}
			if tc.edit != nil {
				tc.edit(&cfg)
			}
			handle := consumerHandler(t, cfg,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &tc.inbox },
				obs.Provider(),
			)

			msg := Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}
			if tc.msgName != "" {
				msg.Name = tc.msgName
			}
			if tc.payload != "" {
				msg.Payload = []byte(tc.payload)
			}
			d := &fakeDelivery{msg: msg, retryErr: tc.retryErr}
			handle(context.Background(), d)

			wantName := tc.wantName
			if wantName == "" {
				wantName = "evt"
			}
			got := obs.Settlements()
			if want := (settlement{Name: wantName, Subscriber: "sub", Settlement: tc.want}); len(got) != 1 || got[0] != want {
				t.Fatalf("reported %+v, want [%+v]", got, want)
			}
			if len(d.settled) != 1 {
				t.Fatalf("settled %v at the broker, want once", d.settled)
			}
		})
	}
}

// TestDeliveryObserverSettledByGoroutine checks that a delivery a handler settles from a goroutine of its
// own, while the consumer settles it too, is reported once, by the one that reached the broker.
func TestDeliveryObserverSettledByGoroutine(t *testing.T) {
	for range 20 {
		obs := &signalObserver{}
		done := make(chan struct{})
		cfg := consumerConfig(HandlerConfig{
			Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler),
			Middlewares: middlewares(func(operation.Handler) operation.Handler {
				return func(ctx *operation.Context) error {
					d := ctx.State.(*State).Delivery
					go func() {
						defer close(done)
						_ = d.Reject(context.Background())
					}()
					return nil
				}
			}),
		})
		handle := consumerHandler(t, cfg,
			func() IBroker { return &fakeBroker{} },
			func() InboxStorage { return &fakeInbox{} },
			obs.Provider(),
		)
		d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`)}}
		handle(context.Background(), d)
		<-done

		got := obs.Settlements()
		if len(got) != 1 || len(d.settled) != 1 {
			t.Fatalf("reported %+v, settled %v at the broker, want one each", got, d.settled)
		}
		want := map[string]SettlementKind{"complete": SettlementCompleted, "reject": SettlementRejected}[d.settled[0]]
		if got[0].Kind != want {
			t.Fatalf("reported %v, but the broker got %s", got[0].Kind, d.settled[0])
		}
	}
}

// TestRelayObserver checks that every batch of the relay is reported, the empty ones included.
func TestRelayObserver(t *testing.T) {
	two := []Message{{ID: "1", Name: "a"}, {ID: "2", Name: "a"}}
	for _, tc := range []struct {
		name   string
		outbox *fakeOutbox
		broker *fakeBroker
		want   RelayBatch // the first batch, without Elapsed
		err    bool
	}{
		{"empty", &fakeOutbox{}, &fakeBroker{}, RelayBatch{}, false},
		{"published", &fakeOutbox{messages: two}, &fakeBroker{}, RelayBatch{Claimed: 2, Published: 2}, false},
		{"partly published", &fakeOutbox{messages: two}, &fakeBroker{failID: "2"}, RelayBatch{Claimed: 2, Published: 1}, false},
		{"claim failed", &fakeOutbox{claimErrs: 1}, &fakeBroker{}, RelayBatch{}, true},
		{"completion failed", &fakeOutbox{messages: two, completeErr: errors.New("database down")}, &fakeBroker{},
			RelayBatch{Claimed: 2, Published: 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obs := &signalObserver{}
			srv, err := ProducerConfig{Interval: time.Millisecond}.New(resolver(t,
				func() IBroker { return tc.broker },
				func() OutboxStorage { return tc.outbox },
				obs.Provider(),
			))
			if err != nil {
				t.Fatal(err)
			}
			if err := srv.Init(context.Background()); err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- srv.Serve() }()
			defer func() {
				_ = srv.Terminate(context.Background())
				<-served
			}()

			deadline := time.Now().Add(5 * time.Second)
			for len(obs.Batches()) < 2 {
				if time.Now().After(deadline) {
					t.Fatalf("reported %d batches", len(obs.Batches()))
				}
				time.Sleep(time.Millisecond)
			}
			got := obs.Batches()[0]
			if got.Claimed != tc.want.Claimed || got.Published != tc.want.Published || (got.Err != nil) != tc.err || got.Elapsed <= 0 {
				t.Fatalf("reported %+v, want %+v with error %v", got, tc.want, tc.err)
			}
		})
	}
}

// TestLockObserver checks that a delivery whose lock another delivery holds is reported with its operation.
func TestLockObserver(t *testing.T) {
	obs := &signalObserver{}
	cfg := consumerConfig(HandlerConfig{
		Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler),
		Lock: operation.Lock{Key: "k", TTL: time.Minute},
	})
	handle := consumerHandler(t, cfg,
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return &fakeInbox{} },
		func() operation.ILocker { return fakeLocker{held: true} },
		obs.Provider(),
	)
	handle(context.Background(), &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`)}})

	locks := obs.Locks()
	if len(locks) != 1 || locks[0] != (observertest.Lock{Type: OperationTypeMessageHandler, Name: "evt", Key: "k-sub-m1", Held: true}) {
		t.Fatalf("reported %+v", locks)
	}
}

// TestUnknownMessageName checks that a message without a handler runs as not_found, with its name logged as
// msg_name, and that msg_elapsed is left out of a message without a creation time.
func TestUnknownMessageName(t *testing.T) {
	var logs bytes.Buffer
	cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)})
	handle := consumerHandler(t, cfg,
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return &fakeInbox{} },
	)
	ctx := logger.NewCtx(context.Background(), zerolog.New(&logs))
	handle(ctx, &fakeDelivery{msg: Message{ID: "m1", Name: "order.unknown", Payload: []byte(`{}`)}})
	handle(ctx, &fakeDelivery{msg: Message{ID: "m2", Name: "evt", Payload: []byte(`{"id":"1"}`), CreatedAt: time.Now()}})

	var lines []map[string]any
	for line := range bytes.Lines(logs.Bytes()) {
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, m)
	}
	for _, line := range lines {
		switch line["msg_id"] {
		case "m1":
			if line["op_name"] != notFoundName || line["msg_name"] != "order.unknown" {
				t.Fatalf("logged %v", line)
			}
			if _, ok := line["msg_elapsed"]; ok {
				t.Fatalf("msg_elapsed logged without a creation time: %v", line)
			}
		case "m2":
			if _, ok := line["msg_name"]; ok {
				t.Fatalf("msg_name logged for a handled message: %v", line)
			}
			if _, ok := line["msg_elapsed"]; !ok && line["message"] == "operation completed" {
				t.Fatalf("no msg_elapsed: %v", line)
			}
		}
	}
	if len(lines) != 4 {
		t.Fatalf("logged %d lines, want 4", len(lines))
	}
}
