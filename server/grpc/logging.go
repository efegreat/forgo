package grpc

import (
	"encoding/base64"
	"math"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/redact"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	RequestRecorderHeaderLimit  = 32 * 1024 // 32 KiB max request metadata logged
	RequestRecorderBodyLimit    = 32 * 1024 // 32 KiB max request message logged
	ResponseRecorderHeaderLimit = 32 * 1024 // 32 KiB max response header, and trailer, metadata logged
	ResponseRecorderBodyLimit   = 32 * 1024 // 32 KiB max response message logged

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
func RequestLogFields(limit, headerLimit int, sensitiveFields []string) middleware.EventFields {
	sensitive := newSensitiveFields(sensitiveFields)
	appendValues := sensitive.metadataValuesAppender()
	return func(ctx *operation.Context, event *zerolog.Event) {
		st := ctx.State.(*State)
		rpcCtx := st.stream.Context()

		if p, ok := peer.FromContext(rpcCtx); ok && p.Addr != nil {
			var buf [64]byte
			event.Bytes(logger.RequestAddrLogKey, appendAddr(buf[:0], p.Addr))
		}
		event.
			Str(logger.RequestPathLogKey, st.FullMethod).
			Str(logger.KindLogKey, kindRequest)

		md, _ := metadata.FromIncomingContext(rpcCtx)
		logMetadata(event, logger.RequestHeadersLogKey, md, headerLimit, appendValues)

		if st.Request != nil {
			logMessage(event, logger.RequestBodyLogKey, st.Request, st.logs.request, limit, sensitive.fields)
		}
	}
}

// ResponseLogFields logs a call when its operation ends: the status code the client got, as grpc_code, the
// header and trailer metadata the call set so far, each up to headerLimit bytes, the response message of a
// unary or client-streaming method, up to limit bytes, and the number of messages of each streamed direction:
// req_msgs for a client or bidi stream, resp_msgs for a server or bidi stream. The values of sensitiveFields
// are masked in the metadata and the message, and those of the credential metadata always are. The operation
// ID is sent in the trailer after the operation ends, so it is not part of the trailer logged.
func ResponseLogFields(limit, headerLimit int, sensitiveFields []string) middleware.EventFields {
	sensitive := newSensitiveFields(sensitiveFields)
	appendValues := sensitive.metadataValuesAppender()
	return func(ctx *operation.Context, event *zerolog.Event) {
		st := ctx.State.(*State)
		event.
			Str(logger.GRPCCodeLogKey, st.Code().String()).
			Str(logger.KindLogKey, kindResponse)
		header, trailer := st.metadata.get()
		if header != nil {
			logMetadata(event, logger.ResponseHeadersLogKey, header, headerLimit, appendValues)
		}
		if trailer != nil {
			logMetadata(event, logger.ResponseTrailersLogKey, trailer, headerLimit, appendValues)
		}
		if st.Response != nil {
			logMessage(event, logger.ResponseBodyLogKey, st.Response, st.logs.response, limit, sensitive.fields)
		}
		if st.kind.clientStreams() {
			event.Uint64(logger.RequestMessagesLogKey, st.receivedMsgs.Load())
		}
		if st.kind.serverStreams() {
			event.Uint64(logger.ResponseMessagesLogKey, st.sentMsgs.Load())
		}
	}
}

// streamMessageLogger logs the messages of the streamed directions of a call on lines of their own, at info:
// "stream message received" with req_body, for a client or bidi stream, and "stream message sent" with
// resp_body, for a server or bidi stream, both with stream_seq, the message's number in its direction, from 1.
// Only the first limit messages of each direction are logged, all of them when limit is negative. They are
// masked and capped like the messages the Recorder logs.
//
// It wraps the stream below every other wrapper, so it logs the messages as they come off and go on the wire.
// It skips unary methods, and the operations whose Args turn the Recorder off.
func streamMessageLogger(limit int, sensitiveFields []string) operation.Middleware {
	max := uint64(math.MaxUint64)
	if limit > 0 {
		max = uint64(limit)
	}
	fields := redact.NewFields(sensitiveFields...)
	return func(next operation.Handler) operation.Handler {
		return func(op *operation.Context) error {
			st := op.State.(*State)
			if st.kind == kindUnary || recorderDisabled(op) {
				return next(op)
			}
			st.stream = &loggedStream{ServerStream: st.stream, lg: logger.FromCtx(op), st: st, max: max, fields: fields}
			return next(op)
		}
	}
}

// loggedStream logs the messages of the streamed directions of its call. It sits right above the call's
// callStream, or its replayStream, which already counted a message when it returns, so the count is the
// message's number.
type loggedStream struct {
	grpc.ServerStream
	lg     *zerolog.Logger
	st     *State
	max    uint64
	fields redact.Fields
}

func (s *loggedStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil && s.st.kind.clientStreams() {
		s.log(m, s.st.logs.request, s.st.receivedMsgs.Load(), kindRequest, logger.RequestBodyLogKey, RequestRecorderBodyLimit, "stream message received")
	}
	return err
}

func (s *loggedStream) SendMsg(m any) error {
	err := s.ServerStream.SendMsg(m)
	if err == nil && s.st.kind.serverStreams() {
		s.log(m, s.st.logs.response, s.st.sentMsgs.Load(), kindResponse, logger.ResponseBodyLogKey, ResponseRecorderBodyLimit, "stream message sent")
	}
	return err
}

func (s *loggedStream) log(m any, plan *messageLog, seq uint64, kind, key string, limit int, msg string) {
	if seq > s.max {
		return
	}
	event := s.lg.Info()
	if !event.Enabled() {
		return
	}
	event.Str(logger.KindLogKey, kind).Uint64(logger.StreamSequenceLogKey, seq)
	if pm, ok := m.(proto.Message); ok {
		logMessage(event, key, pm, plan, limit, s.fields)
	}
	event.Msg(msg)
}

// logMessage adds m to event under key, as JSON with its proto field names, the values of fields masked.
// A message larger than limit on the wire is not encoded at all, and only logger.BodyCutLogKey is set: its
// JSON would be larger still. A message protojson cannot encode, such as one with a string that is not
// UTF-8, is not logged. plan, the messageLog of the method's message, reads a message of its type directly.
func logMessage(event *zerolog.Event, key string, m proto.Message, plan *messageLog, limit int, fields redact.Fields) {
	if !event.Enabled() {
		return
	}
	if proto.Size(m) > limit {
		event.Bool(logger.BodyCutLogKey, true)
		return
	}

	bufp := bufferPool.Get().(*[]byte)
	buf, err := appendMessageJSON((*bufp)[:0], m, plan, fields)
	switch {
	case err == nil && len(buf) <= limit:
		event.RawJSON(key, buf)
	case err == nil:
		// Masked already. LogJSON logs what fits as a string, with logger.BodyCutLogKey.
		fields.LogJSON(event, key, buf[:limit], true)
	case err == errTooDeep:
		// LogJSON logs what it can follow, as a string.
		if buf, err = messageJSON.MarshalAppend(buf[:0], m); err == nil {
			fields.LogJSON(event, key, buf[:min(len(buf), limit)], len(buf) > limit)
		}
	}
	if cap(buf) <= maxPooledBuffer {
		*bufp = buf[:0]
		bufferPool.Put(bufp)
	}
}

// appendAddr appends addr as its String method writes it, without allocating for a TCP address.
func appendAddr(dst []byte, addr net.Addr) []byte {
	if a, ok := addr.(*net.TCPAddr); ok && a.Zone == "" && a.Port >= 0 && a.Port <= math.MaxUint16 {
		// net.IP writes an IPv4-mapped address as IPv4.
		if ip, ok := netip.AddrFromSlice(a.IP); ok {
			return netip.AddrPortFrom(ip.Unmap(), uint16(a.Port)).AppendTo(dst)
		}
	}
	return append(dst, addr.String()...)
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
