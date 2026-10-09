package worker

import (
	"forgo/internal/headerlog"
	"forgo/logger"
	"forgo/operation"
	"forgo/types"
	"time"

	"github.com/rs/zerolog"
)

const (
	PayloadRecorderHeaderLimit = 32 * 1024 // 32 KiB max body logged
	PayloadRecorderBodyLimit   = 32 * 1024 // 32 KiB max body logged
)

var SensitiveHeaders = map[string]struct{}{}

func LoggerFieldAdder() logger.FieldAdder {
	return func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		return zCtx.Str(logger.MessageIDLogKey, msg.ID)
	}
}

func BeforeMessageLogFields(limit, headerLimit int) logger.EventFields {
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		logHeaders(event, msg.Headers, headerLimit)

		payload, truncated := readPayload(msg.Payload, limit)
		if len(payload) > 0 {
			if !truncated {
				event.RawJSON(logger.MessagePayloadLogKey, payload)
			} else {
				event.Str(logger.MessagePayloadLogKey, string(payload))
				event.Bool(logger.BodyCutLogKey, true)
			}
		}
	}
}

func AfterMessageLogFields() logger.EventFields {
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		event.Float64(logger.MessageElapsedLogKey, time.Since(msg.CreatedAt).Seconds())
	}
}

// logHeaders adds the message headers to event. It reads them under the map's read lock instead of
// cloning them.
func logHeaders(event *zerolog.Event, headers *types.JSONMap[string], limit int) {
	// Stays on the stack for typical header counts.
	list := make([]headerlog.Header[string], 0, 32)
	headers.Range(func(name, value string) bool {
		list = append(list, headerlog.Header[string]{Name: name, Value: value})
		return true
	})
	headerlog.Log(event, logger.MessageHeadersLogKey, list, limit, appendHeaderValue)
}

// appendHeaderValue appends one header value as a JSON string, masking SensitiveHeaders.
func appendHeaderValue(buf []byte, name, value string) []byte {
	if _, sensitive := SensitiveHeaders[name]; sensitive {
		value = "[*]"
	}
	return headerlog.AppendString(buf, value)
}

func readPayload(payload []byte, limit int) ([]byte, bool) {
	if len(payload) > limit {
		return payload[:limit], true
	}
	return payload, false
}
