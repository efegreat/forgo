package worker

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/go-playground/validator/v10"
)

// defaultBinderFor returns the default binder without custom validation tags.
func defaultBinderFor(t *testing.T) IBinder {
	t.Helper()
	b, err := newDefaultBinder(nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// bindAs binds payload into a request of type T, and returns the request.
func bindAs[T any](payload string) func(IBinder) (any, error) {
	return func(b IBinder) (any, error) {
		var req T
		err := b.Bind(Message{Payload: []byte(payload)}, &req)
		return req, err
	}
}

// TestDefaultBinder checks that the default binder validates struct requests, through pointers too, and
// only decodes the others, and that it does not decode an empty payload.
func TestDefaultBinder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bind    func(IBinder) (any, error)
		want    any
		wantErr string
	}{
		{name: "struct", bind: bindAs[event](`{"id":"1"}`), want: event{ID: "1"}},
		{name: "invalid struct", bind: bindAs[event](`{}`), wantErr: "required"},
		{name: "malformed payload", bind: bindAs[event](`{`), wantErr: "unexpected EOF"},
		{name: "empty payload", bind: bindAs[struct{}](``), want: struct{}{}},
		{name: "empty payload, invalid struct", bind: bindAs[event](``), wantErr: "required"},
		{name: "pointer", bind: bindAs[*event](`{"id":"1"}`), want: &event{ID: "1"}},
		{name: "invalid pointer", bind: bindAs[*event](`{}`), wantErr: "required"},
		{name: "empty payload, pointer", bind: bindAs[*event](``), want: (*event)(nil)},
		{name: "raw", bind: bindAs[json.RawMessage](`{"a":1}`), want: json.RawMessage(`{"a":1}`)},
		{name: "slice", bind: bindAs[[]event](`[{"id":"1"},{}]`), want: []event{{ID: "1"}, {}}},
		{name: "map", bind: bindAs[map[string]any](`{"a":1}`), want: map[string]any{"a": json.Number("1")}},
		{name: "time", bind: bindAs[time.Time](`"2026-01-02T03:04:05Z"`), want: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.bind(defaultBinderFor(t))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("bound %#v, want %#v", got, tc.want)
			}
		})
	}
}

type priced struct {
	Currency string `json:"currency" validate:"currency"`
}

// TestConsumerValidators checks that ConsumerConfig.Validators registers custom tags in the default
// binder, and that New rejects an invalid one.
func TestConsumerValidators(t *testing.T) {
	newHandler := func() Handler[priced] {
		return func(context.Context, priced) error { return nil }
	}
	currency := func(fl validator.FieldLevel) bool { return fl.Field().String() == "EUR" }

	_, err := ConsumerConfig{Validators: map[string]validator.Func{"": currency}}.New(resolver(t,
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return &fakeInbox{} },
	))
	if err == nil || !strings.Contains(err.Error(), `worker: validator ""`) {
		t.Fatalf("error %v, want one naming the validator", err)
	}

	for _, tc := range []struct{ payload, want string }{
		{`{"currency":"EUR"}`, "complete"},
		{`{"currency":"USD"}`, "reject"},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			cfg := consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[priced](newHandler)})
			cfg.Validators = map[string]validator.Func{"currency": currency}
			handle := consumerHandler(t, cfg,
				func() IBroker { return &fakeBroker{} },
				func() InboxStorage { return &fakeInbox{} },
			)

			d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(tc.payload), Headers: types.NewJSONMap[string]()}}
			handle(context.Background(), d)
			if !slices.Equal(d.settled, []string{tc.want}) {
				t.Fatalf("settled %v, want [%s]", d.settled, tc.want)
			}
		})
	}
}

// TestHandlerReceivesNonStructRequests checks, end to end, that the messages of a request type that is
// not a struct are handled, and so are empty payloads.
func TestHandlerReceivesNonStructRequests(t *testing.T) {
	var got []json.RawMessage
	newHandler := func() Handler[json.RawMessage] {
		return func(ctx context.Context, req json.RawMessage) error {
			if _, ok := operation.FromCtx(ctx); !ok {
				t.Error("handler context belongs to no operation")
			}
			got = append(got, req)
			return nil
		}
	}
	handle := consumerHandler(t, consumerConfig(HandlerConfig{Name: "evt", TransactionDisabled: true, Handler: HandlerAs[json.RawMessage](newHandler)}),
		func() IBroker { return &fakeBroker{} },
		func() InboxStorage { return &fakeInbox{} },
	)
	for _, payload := range []string{`{"id":"1"}`, ``} {
		d := &fakeDelivery{msg: Message{ID: "m1", Name: "evt", Payload: []byte(payload), Headers: types.NewJSONMap[string]()}}
		handle(context.Background(), d)
		if !slices.Equal(d.settled, []string{"complete"}) {
			t.Fatalf("payload %q settled %v, want [complete]", payload, d.settled)
		}
	}
	if len(got) != 2 || string(got[0]) != `{"id":"1"}` || got[1] != nil {
		t.Fatalf("handler received %q", got)
	}
}
