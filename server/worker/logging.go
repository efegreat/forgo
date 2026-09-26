package worker

import (
	"encoding/json"
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

		headers, truncated := readHeaders(msg.Headers, headerLimit)
		if len(headers) > 0 {
			if !truncated {
				event.RawJSON(logger.MessageHeadersLogKey, headers)
			} else {
				event.Str(logger.MessageHeadersLogKey, string(headers))
				event.Bool(logger.HeadersCutLogKey, true)
			}
		}

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

func readHeaders(headers *types.Map, limit int) ([]byte, bool) {
	result := headers.Clone()
	for name := range result {
		if _, sensitive := SensitiveHeaders[name]; sensitive {
			result[name] = "[*]"
		}
	}
	payload, _ := json.Marshal(result)
	if len(payload) > limit {
		return payload[:limit], true
	}
	return payload, false
}

func readPayload(payload []byte, limit int) ([]byte, bool) {
	if len(payload) > limit {
		return payload[:limit], true
	}
	return payload, false
}
