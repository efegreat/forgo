package worker

import (
	"context"
	"io"
	"testing"

	"forgo/operation"
	"forgo/types"

	"github.com/rs/zerolog"
)

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
	fields := BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit)
	lg := zerolog.New(io.Discard)

	b.ReportAllocs()
	for b.Loop() {
		e := lg.Info()
		fields(opCtx, e)
		e.Msg("operation started")
	}
}
