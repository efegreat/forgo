package worker

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

// BenchmarkHandleMessage measures the consumer's work for one message that is handled successfully,
// with fakes for the broker and the inbox, with and without the base chain.
func BenchmarkHandleMessage(b *testing.B) {
	for _, bc := range []struct {
		name    string
		disable bool
	}{
		{"base", false},
		{"bare", true},
	} {
		b.Run(bc.name, func(b *testing.B) {
			cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)})
			cfg.DisableBaseMiddlewares = bc.disable
			boot := forgo.NewBootstrap(nil, []any{
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return benchInbox{} },
			})
			r, err := boot.Build()
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = boot.Close() }()
			srv, err := cfg.New(r)
			if err != nil {
				b.Fatal(err)
			}
			s := srv.(*consumerServer)
			if err := s.Init(logger.NewCtx(context.Background(), zerolog.New(io.Discard))); err != nil {
				b.Fatal(err)
			}
			handle := s.subscribers[0].handle
			msg := Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}
			ctx := s.subscribers[0].ctx

			b.ReportAllocs()
			for b.Loop() {
				handle(ctx, benchDelivery{msg: msg})
			}
		})
	}
}

// benchInbox is an InboxStorage that holds nothing, for benchmarks.
type benchInbox struct{}

func (benchInbox) Status(context.Context, string, string) (InboxStatus, bool, error) {
	return 0, false, nil
}
func (benchInbox) IncrementAttempt(context.Context, string, string) (int, error) { return 1, nil }
func (benchInbox) MarkCompleted(context.Context, string, string) error           { return nil }
func (benchInbox) MarkFailed(context.Context, string, string) error              { return nil }
func (benchInbox) MarkRejected(context.Context, string, string) error            { return nil }
func (benchInbox) DeleteBefore(context.Context, time.Time, int) (int64, error)   { return 0, nil }

// benchDelivery is an IDelivery that settles nothing, for benchmarks.
type benchDelivery struct{ msg Message }

func (d benchDelivery) Message() Message               { return d.msg }
func (benchDelivery) Complete(context.Context) error   { return nil }
func (benchDelivery) Retry(context.Context, int) error { return nil }
func (benchDelivery) Reject(context.Context) error     { return nil }

// BenchmarkMessageLogFields measures the fields the Recorder logs before a message handler runs.
func BenchmarkMessageLogFields(b *testing.B) {
	msg := NewMessageFromDelivery("orders", "orders.created", []byte(`{"id":"ord_123","total":4200}`),
		types.NewJSONMapWithData(map[string]string{
			MessageIDHeader:        "01a11d60-2887-77e7-9adc-75ddcaecee37",
			MessageNameHeader:      "order_created",
			MessageTimestampHeader: "1760000000000000000",
			"content-type":         "application/json",
			"traceparent":          "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		}))
	opCtx := &operation.Context{Context: context.Background(), State: &State{Delivery: &fakeDelivery{msg: msg}}}
	lg := zerolog.New(io.Discard)

	for _, bc := range []struct {
		name      string
		sensitive []string
	}{
		{"none", nil},
		{"masked", []string{"total", "traceparent"}},
	} {
		fields := BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit, bc.sensitive)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				e := lg.Info()
				fields(opCtx, e)
				e.Msg("operation started")
			}
		})
	}
}
