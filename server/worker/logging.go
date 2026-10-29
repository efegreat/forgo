package worker

import (
	"slices"
	"time"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/redact"
	"github.com/efegreat/forgo/safemap"

	"github.com/rs/zerolog"
)

// The limits of what the consumer's Recorder logs of a message.
const (
	PayloadRecorderHeaderLimit = 32 * 1024 // 32 KiB max headers logged
	PayloadRecorderBodyLimit   = 32 * 1024 // 32 KiB max payload logged
)

// LoggerFieldAdder adds the message's ID to the logger of its operation, as msg_id. It adds the message's
// name as msg_name when the operation has another name: a message whose name has no handler runs as the
// operation not_found.
func LoggerFieldAdder() middleware.FieldAdder {
	return func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		zCtx = zCtx.Str(logger.MessageIDLogKey, msg.ID)
		if msg.Name != ctx.Name {
			zCtx = zCtx.Str(logger.MessageNameLogKey, msg.Name)
		}
		return zCtx
	}
}

// BeforeMessageLogFields logs a message before it is handled: its headers and up to limit bytes of
// its payload. The values of sensitiveFields are masked in both, and those of the credential
// headers (Authorization, Cookie, ...) always are.
func BeforeMessageLogFields(limit, headerLimit int, sensitiveFields []string) middleware.EventFields {
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

// AfterMessageLogFields logs a message once it is handled: the time since its CreatedAt, unless it is zero,
// and the attempt that was handled, 0 when the operation ended before the attempt was counted.
func AfterMessageLogFields() middleware.EventFields {
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		if !msg.CreatedAt.IsZero() {
			event.Float64(logger.MessageElapsedLogKey, time.Since(msg.CreatedAt).Seconds())
		}
		event.Int(logger.MessageAttemptLogKey, state.Attempts)
	}
}

// logHeaders adds the message headers to event, nil headers as none. It reads them under the map's read
// lock instead of cloning them.
func logHeaders(
	event *zerolog.Event,
	headers *safemap.JSONMap[string],
	limit int,
	appendValue headerlog.ValueAppender[string],
) {
	// Stays on the stack for typical header counts.
	list := make([]headerlog.Header[string], 0, 32)
	if headers != nil {
		headers.Range(func(name, value string) bool {
			list = append(list, headerlog.Header[string]{Name: name, Value: value})
			return true
		})
	}
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
