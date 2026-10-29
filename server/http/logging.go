package http

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

const (
	RequestRecorderHeaderLimit  = 32 * 1024 // 32 KiB max request header logged
	RequestRecorderBodyLimit    = 32 * 1024 // 32 KiB max request body logged
	ResponseRecorderHeaderLimit = 32 * 1024 // 32 KiB max response header logged
	ResponseRecorderBodyLimit   = 32 * 1024 // 32 KiB max response body logged

	kindRequest  = "request"
	kindResponse = "response"
)

// RequestLogFields logs a request when it starts: its address, path, query, method, headers and
// up to limit bytes of its body. The values of sensitiveFields are masked in all of them, and those
// of the credential headers (Authorization, Cookie, ...) always are.
func RequestLogFields(limit, headerLimit int, sensitiveFields []string) middleware.EventFields {
	sensitive := newSensitiveFields(sensitiveFields)
	appendHeaderValues := sensitive.headerValuesAppender()
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		r := state.Request

		event.Str(logger.RequestAddrLogKey, r.RemoteAddr)
		sensitive.logPath(event, r)
		sensitive.logQuery(event, r.URL.RawQuery)
		event.
			Str(logger.RequestMethodLogKey, r.Method).
			Str(logger.KindLogKey, kindRequest)

		logHeaders(event, logger.RequestHeadersLogKey, r.Header, headerLimit, appendHeaderValues)

		body, truncated, err := readRequestBody(r, limit)
		sensitive.fields.LogJSON(event, logger.RequestBodyLogKey, body, truncated)
		if err != nil {
			event.Str(logger.RequestBodyReadErrLogKey, err.Error())
		}
	}
}

// ResponseLogFields logs a response when its operation ends: its status, headers and body. The
// values of sensitiveFields are masked in all of them, and those of the credential headers always are.
func ResponseLogFields(headerLimit int, sensitiveFields []string) middleware.EventFields {
	sensitive := newSensitiveFields(sensitiveFields)
	appendHeaderValues := sensitive.headerValuesAppender()
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		resp := state.Writer

		event.
			Int(logger.ResponseStatusLogKey, resp.StatusCode()).
			Str(logger.KindLogKey, kindResponse)

		logHeaders(event, logger.ResponseHeadersLogKey, resp.Header(), headerLimit, appendHeaderValues)

		sensitive.fields.LogJSON(event, logger.ResponseBodyLogKey, resp.Body(), resp.BodyTruncated())
	}
}

// logHeaders adds headers to event under key.
func logHeaders(
	event *zerolog.Event,
	key string,
	headers http.Header,
	limit int,
	appendValues headerlog.ValueAppender[[]string],
) {
	var list []headerlog.Header[[]string] // a nil http.Header stays nil and is logged as null
	if headers != nil {
		// Stays on the stack for typical header counts.
		list = make([]headerlog.Header[[]string], 0, 32)
		for name, values := range headers {
			list = append(list, headerlog.Header[[]string]{Name: name, Value: values})
		}
	}
	headerlog.Log(event, key, list, limit, appendValues)
}

// readAheadSize is the buffer readRequestBody starts with when the request has no Content-Length.
const readAheadSize = 512

// replayBody is a request body that readRequestBody read the start of, for the log. It returns those
// bytes first, then the rest of the original body, so the handler reads the body as sent.
type replayBody struct {
	read []byte // what was read ahead and not yet returned
	rest io.ReadCloser
}

func (b *replayBody) Read(p []byte) (int, error) {
	if len(b.read) > 0 {
		n := copy(p, b.read)
		b.read = b.read[n:]
		return n, nil
	}
	return b.rest.Read(p)
}

func (b *replayBody) Close() error {
	return b.rest.Close()
}

// readRequestBody reads up to limitBytes of the body of r, and replaces the body with a replayBody that
// returns them again. It reads one byte more, to tell a body of exactly limitBytes from a longer one, into
// one buffer sized from Content-Length when the request has one. It returns a read error with the bytes
// read before it, and the handler gets the same error after them.
func readRequestBody(
	r *http.Request,
	limitBytes int,
) (
	body []byte,
	truncated bool,
	err error,
) {
	if r.Body == nil || r.Body == http.NoBody || limitBytes <= 0 {
		return nil, false, nil
	}

	want := limitBytes + 1
	size := min(want, readAheadSize)
	if r.ContentLength >= 0 {
		// one more byte than the body, so that the read that finds its end needs no room of its own
		size = int(min(int64(want), r.ContentLength+1))
	}
	buf := make([]byte, 0, size)
	for len(buf) < want {
		if len(buf) == cap(buf) {
			buf = slices.Grow(buf, 1)
		}
		n, readErr := r.Body.Read(buf[len(buf):min(cap(buf), want)])
		buf = buf[:len(buf)+n]
		if readErr != nil {
			if readErr != io.EOF {
				err = readErr
			}
			break
		}
	}
	r.Body = &replayBody{read: buf, rest: r.Body}

	if len(buf) > limitBytes {
		return buf[:limitBytes], true, err
	}
	return buf, false, err
}

func newResponseRecorder(
	w http.ResponseWriter,
	bodyLimit int,
) *ResponseRecorder {
	return &ResponseRecorder{
		ResponseWriter: w,
		bodyLimit:      bodyLimit,
	}
}

// ResponseRecorder is the http.ResponseWriter of an operation, in State.Writer. It writes through to the client
// and records the status and the head of the body for the Recorder's log. Reach the optional interfaces of the
// writer below it, such as http.Flusher, with http.NewResponseController.
type ResponseRecorder struct {
	http.ResponseWriter

	statusCode int

	body          []byte
	bodyLimit     int
	bodyTruncated bool
	hijacked      bool

	// hijacks is the server's, which Hijack reports to. It is nil outside the server.
	hijacks *hijacks
}

// Unwrap returns the http.ResponseWriter below the recorder, for http.ResponseController.
func (r *ResponseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// Hijack lets the handler take the connection over, such as for a WebSocket, through http.NewResponseController
// or http.Hijacker. The handler then answers on the connection, so the server writes no response, and
// Terminate waits for the handler to return, while Kill closes the connection. Like net/http, it fails on
// HTTP/2 and HTTP/3, with http.ErrNotSupported, and on a connection hijacked already, with http.ErrHijacked.
func (r *ResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if r.hijacked {
		return nil, nil, http.ErrHijacked
	}
	if r.hijacks != nil {
		r.hijacks.start(r)
	}
	c, rw, err := http.NewResponseController(r.ResponseWriter).Hijack()
	if err != nil {
		if r.hijacks != nil {
			r.hijacks.done(r)
		}
		return nil, nil, err
	}
	r.hijacked = true
	if r.hijacks != nil {
		r.hijacks.hijacked(r, c)
	}
	return c, rw, nil
}

// WriteHeader sends the status of the response. Informational statuses, such as 103 Early Hints, may come
// first. Once a final status is sent, later ones are dropped.
func (r *ResponseRecorder) WriteHeader(statusCode int) {
	if r.statusCode != 0 {
		return
	}
	if statusCode >= 100 && statusCode < 200 && statusCode != http.StatusSwitchingProtocols {
		r.ResponseWriter.WriteHeader(statusCode)
		return
	}

	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

// Write writes p to the body of the response, after sending 200 when no status is sent yet.
func (r *ResponseRecorder) Write(p []byte) (int, error) {
	if r.statusCode == 0 {
		// net/http implicitly sends 200 when Write is called before
		// WriteHeader.
		r.statusCode = http.StatusOK
	}

	r.capture(p)

	return r.ResponseWriter.Write(p)
}

func (r *ResponseRecorder) capture(p []byte) {
	if r.bodyLimit <= 0 {
		if len(p) > 0 {
			r.bodyTruncated = true
		}
		return
	}

	remaining := r.bodyLimit - len(r.body)
	if remaining <= 0 {
		if len(p) > 0 {
			r.bodyTruncated = true
		}
		return
	}

	if len(p) <= remaining {
		r.body = append(r.body, p...)
		return
	}

	r.body = append(r.body, p[:remaining]...)
	r.bodyTruncated = true
}

// StatusCode returns the status sent, or 0 while the response is not written. A handler that hijacked the
// connection answers on it, so the status is 0 unless it sent one through the recorder first, such as the
// 101 Switching Protocols of some WebSocket libraries.
func (r *ResponseRecorder) StatusCode() int {
	return r.statusCode
}

// Written reports whether the response is started, with a status sent or a write to its body, or whether the
// handler hijacked the connection. The server answers only operations whose response is not written.
func (r *ResponseRecorder) Written() bool {
	return r.statusCode != 0 || r.hijacked
}

// Body returns the head of the body written so far, up to the recorder's limit.
func (r *ResponseRecorder) Body() []byte {
	return r.body
}

// BodyTruncated reports whether more of the body was written than Body holds.
func (r *ResponseRecorder) BodyTruncated() bool {
	return r.bodyTruncated
}

// defaultHTTPErrorLog writes the lines of net/http's error log as error logs of the server's logger.
type defaultHTTPErrorLog struct {
	l zerolog.Logger
}

func (w defaultHTTPErrorLog) Write(p []byte) (int, error) {
	l := len(p)
	if l > 0 && p[l-1] == '\n' {
		p = p[:l-1]
	}
	w.l.Error().Err(errors.New(string(p))).Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).Msg("http server error")
	return l, nil
}
