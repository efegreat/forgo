package worker

import (
	"slices"
	"time"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/internal/redact"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"

	"github.com/rs/zerolog"
)

const (
	PayloadRecorderHeaderLimit = 32 * 1024 // 32 KiB max headers logged
	PayloadRecorderBodyLimit   = 32 * 1024 // 32 KiB max payload logged
)

func LoggerFieldAdder() logger.FieldAdder {
	return func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		return zCtx.Str(logger.MessageIDLogKey, msg.ID)
	}
}

// BeforeMessageLogFields logs a message before it is handled: its headers and up to limit bytes of
// its payload. The values of sensitiveFields are masked in both, and those of the credential
// headers (Authorization, Cookie, ...) always are.
func BeforeMessageLogFields(limit, headerLimit int, sensitiveFields []string) logger.EventFields {
	sensitive := redact.NewFields(sensitiveFields...)
	sensitiveHeaders := redact.NewFields(slices.Concat(redact.CredentialHeaders, sensitiveFields)...)
	appendHeaderValue := headerValueAppender(sensitiveHeaders)
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

// readPayload cuts payload at limit, a negative limit counting as 0.
func readPayload(payload []byte, limit int) ([]byte, bool) {
	limit = max(limit, 0)
	if len(payload) > limit {
		return payload[:limit], true
	}
	return payload, false
}
