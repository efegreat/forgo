package http

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/logger"
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
func RequestLogFields(limit, headerLimit int, sensitiveFields []string) logger.EventFields {
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
func ResponseLogFields(headerLimit int, sensitiveFields []string) logger.EventFields {
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

type replayBody struct {
	io.Reader
	io.Closer
}

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

	original := r.Body

	// Read one extra byte so we can distinguish:
	//
	// exactly limitBytes
	// from
	// more than limitBytes.
	buf, err := io.ReadAll(
		io.LimitReader(original, int64(limitBytes)+1),
	)

	// Restore everything we consumed.
	//
	// buf contains the already-consumed prefix, while original now
	// points at the unread remainder of the body.
	r.Body = replayBody{
		Reader: io.MultiReader(
			bytes.NewReader(buf),
			original,
		),
		Closer: original,
	}

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
}

// Unwrap returns the http.ResponseWriter below the recorder, for http.ResponseController.
func (r *ResponseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
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

// StatusCode returns the status sent, or 0 while the response is not written.
func (r *ResponseRecorder) StatusCode() int {
	return r.statusCode
}

// Written reports whether the response is started, with a status sent or a write to its body. The server
// answers only operations whose response is not written.
func (r *ResponseRecorder) Written() bool {
	return r.statusCode != 0
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
