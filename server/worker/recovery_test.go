package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

// TestConsumerLogsFailureCauses checks that a panic is logged with its stack, and that an error
// without a cause is logged with its message.
func TestConsumerLogsFailureCauses(t *testing.T) {
	for _, tc := range []struct {
		name    string
		locker  fakeLocker
		edit    func(*ConsumerConfig)
		wantMsg string
		wantErr string
		stack   bool
	}{
		{
			name: "panic", wantMsg: "operation failed", wantErr: "panic: boom", stack: true,
			edit: func(c *ConsumerConfig) {
				c.Subscribers[0].Handlers[0].Handler = HandlerAs[event](func() Handler[event] {
					return func(context.Context, event) error { panic("boom") }
				})
			},
		},
		{
			name: "lock held", locker: fakeLocker{held: true}, wantMsg: "message left unsettled, retrying", wantErr: errLocked.Message,
			edit: func(c *ConsumerConfig) {
				c.Subscribers[0].Handlers[0].Lock = operation.Lock{Key: "k", TTL: time.Minute}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ConsumerConfig{Subscribers: []SubscriberConfig{{
				Name:     "sub",
				Handlers: []HandlerConfig{{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler)}},
			}}}
			tc.edit(&cfg)
			handle := consumerHandler(t, cfg,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &fakeInbox{} },
				func() operation.ILocker { return tc.locker },
			)

			var logs bytes.Buffer
			d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}}
			handle(logger.NewCtx(context.Background(), zerolog.New(&logs)), d)

			var logged string
			for line := range bytes.Lines(logs.Bytes()) {
				var fields map[string]any
				if err := json.Unmarshal(line, &fields); err != nil {
					t.Fatalf("invalid log line %s: %v", line, err)
				}
				if fields["message"] == tc.wantMsg {
					logged, _ = fields["error"].(string)
				}
			}
			if !strings.HasPrefix(logged, tc.wantErr) || strings.Contains(logged, "goroutine") != tc.stack {
				t.Fatalf("%q logged error %q, want %q (stack %v):\n%s", tc.wantMsg, logged, tc.wantErr, tc.stack, &logs)
			}
		})
	}
}
