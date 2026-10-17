package worker

import (
	"context"
	"io"
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
		{name: "unknown name", msgName: "other", wantName: "other", wantCode: errNoHandler.Code},
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
