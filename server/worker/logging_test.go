package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"testing"

	"forgo/logger"
	"forgo/operation"
	"forgo/types"

	"github.com/rs/zerolog"
)

// marshalHeaders is how message headers were logged before headerlog: mask a clone, json.Marshal.
func marshalHeaders(headers map[string]string) []byte {
	result := maps.Clone(headers)
	for name := range result {
		if _, sensitive := SensitiveHeaders[name]; sensitive {
			result[name] = "[*]"
		}
	}
	payload, _ := json.Marshal(result)
	return payload
}

func TestMessageLogFieldsHeaders(t *testing.T) {
	SensitiveHeaders["x-token"] = struct{}{}
	t.Cleanup(func() { delete(SensitiveHeaders, "x-token") })

	cases := map[string]map[string]string{
		"empty":  {},
		"masked": {"x-token": "secret", "X-Token": "not exact", MessageNameHeader: "order_created"},
		"tricky": {"x-html": "<a>&b", "x-bad": "bad\xffutf8", "é": "日本\u2028"},
		"many":   manyHeaders(40),
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			if got, want := loggedMessageHeaders(t, types.NewJSONMapWithData(h)), marshalHeaders(h); !bytes.Equal(got, want) {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// TestMessageLogFieldsNullHeaders covers headers scanned from SQL NULL: Range cannot tell them from
// an empty map, so they log as {}.
func TestMessageLogFieldsNullHeaders(t *testing.T) {
	headers := types.NewJSONMap[string]()
	if err := headers.Scan(nil); err != nil {
		t.Fatal(err)
	}
	if got := loggedMessageHeaders(t, headers); string(got) != "{}" {
		t.Fatalf("got %s, want {}", got)
	}
}

// loggedMessageHeaders returns the msg_headers JSON the Recorder logs before a message with headers.
func loggedMessageHeaders(t *testing.T, headers *types.JSONMap[string]) []byte {
	t.Helper()
	msg := Message{Name: "order_created", Headers: headers, Payload: []byte(`{"id":1}`)}
	opCtx := &operation.Context{Context: context.Background(), State: &State{Delivery: &fakeDelivery{msg: msg}}}

	var out bytes.Buffer
	lg := zerolog.New(&out)
	e := lg.Info()
	BeforeMessageLogFields(PayloadRecorderBodyLimit, PayloadRecorderHeaderLimit)(opCtx, e)
	e.Send()

	var line map[string]json.RawMessage
	if err := json.Unmarshal(out.Bytes(), &line); err != nil {
		t.Fatalf("invalid log line %s: %v", out.Bytes(), err)
	}
	return line[logger.MessageHeadersLogKey]
}

func manyHeaders(n int) map[string]string {
	h := map[string]string{}
	for i := range n {
		h[fmt.Sprintf("x-header-%02d", i)] = fmt.Sprintf("value-%d", i)
	}
	return h
}
