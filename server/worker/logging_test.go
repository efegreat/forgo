package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/efegreat/forgo/internal/redact"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

// marshalHeaders is how message headers were logged before headerlog: mask a clone, json.Marshal.
// Sensitive names match like strings.EqualFold, and the credential headers are always masked.
func marshalHeaders(headers map[string]string, sensitive ...string) []byte {
	result := maps.Clone(headers)
	for name := range result {
		for _, s := range slices.Concat(redact.CredentialHeaders, sensitive) {
			if strings.EqualFold(name, s) {
				result[name] = "[*]"
			}
		}
	}
	payload, _ := json.Marshal(result)
	return payload
}

func TestMessageLogFieldsHeaders(t *testing.T) {
	cases := map[string]map[string]string{
		"empty":  {},
		"masked": {"x-token": "secret", "X-Token": "any case", MessageNameHeader: "order_created"},
		"credentials": {
			"authorization": "Bearer secret", "Cookie": "session=secret", "x-api-key": "secret", "x-trace": "kept",
		},
		"tricky": {"x-html": "<a>&b", "x-bad": "bad\xffutf8", "é": "日本\u2028"},
		"many":   manyHeaders(40),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			got := loggedMessage(t, Message{Headers: types.NewJSONMapWithData(h)}, "x-token")[logger.MessageHeadersLogKey]
			if want := marshalHeaders(h, "x-token"); !bytes.Equal(got, want) {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// TestMessageLogFieldsNullHeaders covers headers scanned from SQL NULL: Range cannot tell them from
// an empty map, so they log as {}. Nil headers, from a custom broker adapter, log as {} too.
func TestMessageLogFieldsNullHeaders(t *testing.T) {
	headers := types.NewJSONMap[string]()
	if err := headers.Scan(nil); err != nil {
		t.Fatal(err)
	}
	for _, h := range []*types.JSONMap[string]{headers, nil} {
		if got := loggedMessage(t, Message{Headers: h})[logger.MessageHeadersLogKey]; string(got) != "{}" {
			t.Fatalf("got %s, want {}", got)
		}
	}
}

func TestMessageLogFieldsPayload(t *testing.T) {
	for _, tc := range []struct {
		name, payload, want string
		sensitive           []string
	}{
		{"raw", `{"id":"ord_1","card":{"number":"4111"}}`, `{"id":"ord_1","card":{"number":"4111"}}`, nil},
		{"masked", `{"id":"ord_1","Card":{"number":"4111"}}`, `{"id":"ord_1","Card":"[*]"}`, []string{"card"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := Message{Headers: types.NewJSONMap[string](), Payload: []byte(tc.payload)}
			if got := loggedMessage(t, msg, tc.sensitive...)[logger.MessagePayloadLogKey]; string(got) != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}

// TestMessageLogFieldsCutPayload checks that a payload cut at the logging limit is masked up to
// the cut and logged as a string.
func TestMessageLogFieldsCutPayload(t *testing.T) {
	head := `{"id":"ord_1","card":{"number":"4111111111111111"},"note":"`
	padding := strings.Repeat("x", PayloadRecorderBodyLimit-len(head))
	msg := Message{Headers: types.NewJSONMap[string](), Payload: []byte(head + padding + `"}`)}

	line := loggedMessage(t, msg, "card")
	want, _ := json.Marshal(`{"id":"ord_1","card":"[*]","note":"` + padding)
	if got := line[logger.MessagePayloadLogKey]; !bytes.Equal(got, want) || string(line[logger.BodyCutLogKey]) != "true" {
		t.Fatalf("got %.60s... cut=%s, want %.60s... cut=true", got, line[logger.BodyCutLogKey], want)
	}
}

// TestMessageLogFieldsNegativeLimits checks that negative limits count as 0 instead of panicking.
func TestMessageLogFieldsNegativeLimits(t *testing.T) {
	msg := Message{Headers: types.NewJSONMapWithData(map[string]string{"a": "b"}), Payload: []byte(`{"id":"ord_1"}`)}
	opCtx := &operation.Context{Context: context.Background(), State: &State{Delivery: &fakeDelivery{msg: msg}}}

	var out bytes.Buffer
	lg := zerolog.New(&out)
	e := lg.Info()
	BeforeMessageLogFields(-1, -1, nil)(opCtx, e)
	e.Send()

	var line map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("invalid log line %s: %v", out.Bytes(), err)
	}
	if _, ok := line[logger.MessagePayloadLogKey]; ok || string(line[logger.HeadersCutLogKey]) != "true" {
		t.Fatalf("got %s, want no payload and cut headers", out.Bytes())
	}
}

// loggedMessage returns the fields the Recorder logs before handling msg.
func loggedMessage(t *testing.T, msg Message, sensitive ...string) map[string]json.RawMessage {
	t.Helper()
	opCtx := &operation.Context{Context: context.Background(), State: &State{Delivery: &fakeDelivery{msg: msg}}}

	var out bytes.Buffer
	lg := zerolog.New(&out)
	e := lg.Info()
	BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit, sensitive)(opCtx, e)
	e.Send()

	var line map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("invalid log line %s: %v", out.Bytes(), err)
	}
	return line
}

func manyHeaders(n int) map[string]string {
	h := map[string]string{}
	for i := range n {
		h[fmt.Sprintf("x-header-%02d", i)] = fmt.Sprintf("value-%d", i)
	}
	return h
}

type payment struct {
	ID     string `json:"id" validate:"required"`
	Amount int8   `json:"amount"`
	Pin    int8   `json:"pin"`
}

// TestConsumerMasksBindingErrors checks that the binding error the Recorder logs never quotes the
// input of a sensitive field.
func TestConsumerMasksBindingErrors(t *testing.T) {
	for _, tc := range []struct {
		name, payload, logged string
	}{
		{"sensitive field", `{"id":"1","pin":1000}`, "message binding error: json: cannot unmarshal number into Go struct field payment.pin of type int8"},
		{"field", `{"id":"1","amount":1000}`, "message binding error: json: cannot unmarshal number 1000 into Go struct field payment.amount of type int8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newHandler := func() Handler[payment] {
				return func(context.Context, payment) error { return nil }
			}
			cfg := consumerConfig(HandlerConfig{Name: "pay", TransactionDisabled: true, Handler: HandlerAs[payment](newHandler)})
			cfg.SensitiveFields = []string{"pin"}
			handle := consumerHandler(t, cfg, func() IBroker { return &fakeBroker{} }, func() InboxStorage { return &fakeInbox{} })

			var logs bytes.Buffer
			ctx := logger.NewCtx(context.Background(), zerolog.New(&logs))
			msg := Message{ID: "m1", Name: "pay", Payload: []byte(tc.payload), Headers: types.NewJSONMap[string]()}
			handle(ctx, &fakeDelivery{msg: msg})

			var failed struct {
				Code  string `json:"err_code"`
				Error string `json:"error"`
			}
			lines := bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n"))
			if err := json.Unmarshal(lines[len(lines)-1], &failed); err != nil {
				t.Fatal(err)
			}
			if failed.Code != "msg_bind" || failed.Error != tc.logged {
				t.Fatalf("logged %+v, want msg_bind %q", failed, tc.logged)
			}
		})
	}
}
