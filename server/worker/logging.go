package worker

import (
	"forgo/internal/headerlog"
	"forgo/internal/redact"
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

func LoggerFieldAdder() logger.FieldAdder {
	return func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		return zCtx.Str(logger.MessageIDLogKey, msg.ID)
	}
}

// BeforeMessageLogFields logs a message before it is handled: its headers and up to limit bytes of
// its payload. The values of sensitiveFields are masked in both.
func BeforeMessageLogFields(limit, headerLimit int, sensitiveFields []string) logger.EventFields {
	sensitive := redact.NewFields(sensitiveFields...)
	appendHeaderValue := headerValueAppender(sensitive)
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		logHeaders(event, msg.Headers, headerLimit, appendHeaderValue)

		payload, truncated := readPayload(msg.Payload, limit)
		sensitive.LogJSON(event, logger.MessagePayloadLogKey, payload, truncated)
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
func logHeaders(
	event *zerolog.Event,
	headers *types.JSONMap[string],
	limit int,
	appendValue headerlog.ValueAppender[string],
) {
	// Stays on the stack for typical header counts.
	list := make([]headerlog.Header[string], 0, 32)
	headers.Range(func(name, value string) bool {
		list = append(list, headerlog.Header[string]{Name: name, Value: value})
		return true
	})
	headerlog.Log(event, logger.MessageHeadersLogKey, list, limit, appendValue)
}

// headerValueAppender returns the headerlog.ValueAppender that writes one header value as a JSON
// string, masking the values of sensitive headers.
func headerValueAppender(sensitive redact.Fields) headerlog.ValueAppender[string] {
	return func(buf []byte, name, value string) []byte {
		if sensitive.Has(name) {
			value = redact.Mask
		}
		return headerlog.AppendString(buf, value)
	}
}

func readPayload(payload []byte, limit int) ([]byte, bool) {
	if len(payload) > limit {
		return payload[:limit], true
	}
	return payload, false
}
