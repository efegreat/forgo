package grpc

import (
	"encoding/base64"
	"slices"
	"strings"
	"sync"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/redact"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	RequestRecorderHeaderLimit = 32 * 1024 // 32 KiB max request metadata logged
	RequestRecorderBodyLimit   = 32 * 1024 // 32 KiB max request message logged
	ResponseRecorderBodyLimit  = 32 * 1024 // 32 KiB max response message logged

	kindRequest  = "request"
	kindResponse = "response"

	// binarySuffix ends the keys of binary metadata, whose values are raw bytes.
	binarySuffix = "-bin"
	// maxPooledBuffer keeps a single huge message from pinning its buffer in the pool.
	maxPooledBuffer = 64 * 1024
)

var (
	maskedValues = []string{redact.Mask}

	// messageJSON encodes logged messages with their proto field names, which SensitiveFields name.
	messageJSON = protojson.MarshalOptions{UseProtoNames: true}

	// bufferPool holds the buffers messages are encoded into. A buffer never outlives a log field: the
	// event copies it before it goes back to the pool.
	bufferPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 1024)
			return &buf
		},
	}
)

// RequestLogFields logs a call when it starts: its full method as path, the client's address, its
// metadata and its request message, up to limit and headerLimit bytes. The values of sensitiveFields are
// masked in the metadata and the message, and those of the credential metadata (authorization, cookie,
// ...) always are. Binary metadata is logged in base64.
func RequestLogFields(limit, headerLimit int, sensitiveFields []string) logger.EventFields {
	sensitive := newSensitiveFields(sensitiveFields)
	appendValues := sensitive.metadataValuesAppender()
	return func(ctx *operation.Context, event *zerolog.Event) {
		st := ctx.State.(*State)
		rpcCtx := st.stream.Context()

		if p, ok := peer.FromContext(rpcCtx); ok && p.Addr != nil {
			event.Str(logger.RequestAddrLogKey, p.Addr.String())
		}
		event.
			Str(logger.RequestPathLogKey, st.FullMethod).
			Str(logger.KindLogKey, kindRequest)

		md, _ := metadata.FromIncomingContext(rpcCtx)
		logMetadata(event, logger.RequestHeadersLogKey, md, headerLimit, appendValues)

		if st.Request != nil {
			logMessage(event, logger.RequestBodyLogKey, st.Request, limit, sensitive.fields)
		}
	}
}

// ResponseLogFields logs a call when its operation ends: the status code the client got, as grpc_code,
// and the response message of a unary or client-streaming method, up to limit bytes, with the values of
// sensitiveFields masked.
func ResponseLogFields(limit int, sensitiveFields []string) logger.EventFields {
	sensitive := newSensitiveFields(sensitiveFields)
	return func(ctx *operation.Context, event *zerolog.Event) {
		st := ctx.State.(*State)
		event.
			Str(logger.GRPCCodeLogKey, st.Code().String()).
			Str(logger.KindLogKey, kindResponse)
		if st.Response != nil {
			logMessage(event, logger.ResponseBodyLogKey, st.Response, limit, sensitive.fields)
		}
	}
}

// logMessage adds m to event under key, as JSON with its proto field names, the values of fields masked.
// A message larger than limit on the wire is not encoded at all, and only logger.BodyCutLogKey is set: its
// JSON would be larger still.
func logMessage(event *zerolog.Event, key string, m proto.Message, limit int, fields redact.Fields) {
	if !event.Enabled() {
		return
	}
	if proto.Size(m) > limit {
		event.Bool(logger.BodyCutLogKey, true)
		return
	}

	bufp := bufferPool.Get().(*[]byte)
	buf, err := messageJSON.MarshalAppend((*bufp)[:0], m)
	if err == nil {
		// protojson adds random whitespace, which LogJSON compacts.
		truncated := len(buf) > limit
		fields.LogJSON(event, key, buf[:min(len(buf), limit)], truncated)
	}
	if cap(buf) <= maxPooledBuffer {
		*bufp = buf[:0]
		bufferPool.Put(bufp)
	}
}

// logMetadata adds md to event under key.
func logMetadata(event *zerolog.Event, key string, md metadata.MD, limit int, appendValues headerlog.ValueAppender[[]string]) {
	var list []headerlog.Header[[]string] // nil metadata stays nil and is logged as null
	if md != nil {
		// Stays on the stack for typical metadata counts.
		list = make([]headerlog.Header[[]string], 0, 32)
		for name, values := range md {
			list = append(list, headerlog.Header[[]string]{Name: name, Value: values})
		}
	}
	headerlog.Log(event, key, list, limit, appendValues)
}

// sensitiveFields holds the names whose values are masked in logs.
type sensitiveFields struct {
	// metadata are masked in metadata: the configured names and redact.CredentialHeaders.
	metadata redact.Fields
	// fields are masked in messages.
	fields redact.Fields
}

func newSensitiveFields(names []string) sensitiveFields {
	return sensitiveFields{
		metadata: redact.NewFields(slices.Concat(redact.CredentialHeaders, names)...),
		fields:   redact.NewFields(names...),
	}
}

// metadataValuesAppender returns the headerlog.ValueAppender that writes the values of one metadata key as
// a JSON array, masking the values of sensitive keys and encoding those of binary keys in base64.
func (s sensitiveFields) metadataValuesAppender() headerlog.ValueAppender[[]string] {
	return func(buf []byte, name string, values []string) []byte {
		masked := s.metadata.Has(name)
		if masked {
			values = maskedValues
		}
		if values == nil {
			return append(buf, "null"...)
		}

		binary := !masked && strings.HasSuffix(name, binarySuffix)
		buf = append(buf, '[')
		for i, value := range values {
			if i > 0 {
				buf = append(buf, ',')
			}
			if binary {
				// base64 needs no JSON escaping.
				buf = append(buf, '"')
				buf = base64.StdEncoding.AppendEncode(buf, []byte(value))
				buf = append(buf, '"')
				continue
			}
			buf = headerlog.AppendString(buf, value)
		}
		return append(buf, ']')
	}
}
