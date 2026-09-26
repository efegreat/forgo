package worker

import (
	"forgo/logger"
	"forgo/operation"
	"forgo/types"
	"time"

	"github.com/rs/zerolog"
)

const (
	PayloadRecorderHeaderLimit = 64 * 1024 // 64 KiB max body logged
	PayloadRecorderBodyLimit   = 64 * 1024 // 64 KiB max body logged
)

var SensitiveHeaders = map[string]struct{}{}

func LoggerFieldAdder() logger.FieldAdder {
	return func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		return zCtx.Str(logger.MessageIDLogKey, msg.ID)
	}
}

func BeforeMessageLogFields(limit int) logger.EventFields {
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		msg := state.Delivery.Message()

		event.Interface(logger.MessageHeadersLogKey, loggableHeaders(msg.Headers))
		payload, truncated := readPayload(msg.Payload, limit)

		if len(payload) > 0 {
			event.Str(logger.MessagePayloadLogKey, string(payload))
		}

		if truncated {
			event.Bool(logger.BodyCutLogKey, true)
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

func loggableHeaders(headers *types.Map) map[string]any {
	result := headers.Clone()

	for name := range result {
		if _, sensitive := SensitiveHeaders[name]; sensitive {
			result[name] = "[*]"
		}
	}

	return result
}

func readPayload(payload []byte, limit int) ([]byte, bool) {
	if len(payload) > limit {
		return payload[:limit], true
	}
	return payload, false
}
