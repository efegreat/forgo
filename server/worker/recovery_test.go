package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// TestRecorderDisabledByHandlerArgs checks that ArgKeyRecorderDisabled turns the Recorder off, and
// that a key of another type with the same value does not.
func TestRecorderDisabledByHandlerArgs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   map[any]any
		logged bool
	}{
		{"disabled", map[any]any{ArgKeyRecorderDisabled: true}, false},
		{"enabled", map[any]any{ArgKeyRecorderDisabled: false}, true},
		{"other key type", map[any]any{uint8(ArgKeyRecorderDisabled): true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handle := consumerHandler(t, ConsumerConfig{Subscribers: []SubscriberConfig{{
				Name: "sub",
				Handlers: []HandlerConfig{{
					Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](noopHandler), Args: tc.args,
				}},
			}}},
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &fakeInbox{} },
			)

			var logs bytes.Buffer
			d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}}
			handle(logger.NewCtx(context.Background(), zerolog.New(&logs)), d)

			if logged := bytes.Contains(logs.Bytes(), []byte(`"operation completed"`)); logged != tc.logged {
				t.Fatalf("operation logged %v, want %v:\n%s", logged, tc.logged, &logs)
			}
		})
	}
}

// TestConsumerLogsHandlerErrors checks that the end line of a message logs the handler's error itself,
// with what wraps it and the failure of the step that followed, and the attempt.
func TestConsumerLogsHandlerErrors(t *testing.T) {
	declined := operation.Error{Code: "declined", Message: "card declined"}
	for _, tc := range []struct {
		name     string
		handler  Handler[event]
		retryErr error
		wantMsg  string
		wantErr  string // prefix of the logged error
		wantCode string
		stack    bool
	}{
		{
			name:    "wrapped operation error",
			handler: func(context.Context, event) error { return fmt.Errorf("charge order 7: %w", declined) },
			wantMsg: "operation failed", wantErr: "charge order 7: card declined", wantCode: "declined",
		},
		{
			name:     "failed retry",
			handler:  func(context.Context, event) error { return errors.New("boom") },
			retryErr: errors.New("broker down"),
			wantMsg:  "operation failed", wantErr: "boom; retry: broker down", wantCode: "x",
		},
		{
			name:    "panic of an operation error",
			handler: func(context.Context, event) error { panic(declined) },
			wantMsg: "operation failed", wantErr: "panic: card declined", wantCode: "declined", stack: true,
		},
		{
			name: "delivery settled by the handler",
			handler: func(ctx context.Context, _ event) error {
				op, _ := operation.FromCtx(ctx)
				return op.State.(*State).Delivery.Complete(ctx)
			},
			wantMsg: "operation completed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newHandler := func() Handler[event] { return tc.handler }
			handle := consumerHandler(t, consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[event](newHandler)}),
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &fakeInbox{} },
			)

			var logs bytes.Buffer
			msg := Message{ID: "m1", Name: "evt", Payload: []byte(`{"id":"1"}`), Headers: types.NewJSONMap[string]()}
			handle(logger.NewCtx(context.Background(), zerolog.New(&logs)), &fakeDelivery{msg: msg, retryErr: tc.retryErr})

			var end struct {
				Message string `json:"message"`
				Error   string `json:"error"`
				Code    string `json:"err_code"`
				Attempt *int   `json:"msg_attempt"`
			}
			for line := range bytes.Lines(logs.Bytes()) {
				var fields struct {
					Message string `json:"message"`
				}
				if err := json.Unmarshal(line, &fields); err != nil {
					t.Fatalf("invalid log line %s: %v", line, err)
				}
				if fields.Message == "operation completed" || fields.Message == "operation failed" {
					_ = json.Unmarshal(line, &end)
				}
			}
			if end.Message != tc.wantMsg || !strings.HasPrefix(end.Error, tc.wantErr) || end.Code != tc.wantCode ||
				strings.Contains(end.Error, "goroutine") != tc.stack || end.Attempt == nil || *end.Attempt != 1 {
				t.Fatalf("logged %+v, want %q with error %q, code %q (stack %v) and msg_attempt 1:\n%s",
					end, tc.wantMsg, tc.wantErr, tc.wantCode, tc.stack, &logs)
			}
		})
	}
}
