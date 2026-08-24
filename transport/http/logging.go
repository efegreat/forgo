package http

import (
	"bytes"
	"errors"
	"forgo/logger"
	"forgo/operation"
	"io"
	"net/http"

	"github.com/rs/zerolog"
)

const (
	RequestRecorderBodyLimit  = 64 * 1024 // 64 KiB max request body logged
	ResponseRecorderBodyLimit = 64 * 1024 // 64 KiB max response body logged
	KindRequest               = "request"
	KindResponse              = "response"
)

var SensitiveHeaders = map[string]struct{}{
	"Authorization":       {},
	"Proxy-Authorization": {},
	"Cookie":              {},
	"Set-Cookie":          {},
	"X-Api-Key":           {},
}

func RequestLogFields(limit int) logger.EventFields {
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		r := state.Request

		event.
			Str(logger.RequestAddrLogKey, r.RemoteAddr).
			Str(logger.RequestPathLogKey, r.URL.Path).
			Str(logger.RequestQueryLogKey, r.URL.RawQuery).
			Str(logger.RequestMethodLogKey, r.Method).
			Interface(logger.RequestHeadersLogKey, loggableHeaders(r.Header)).
			Str(logger.KindLogKey, KindRequest)

		body, truncated, err := readRequestBody(r, limit)

		if len(body) > 0 {
			event.Str(logger.RequestBodyLogKey, string(body))
		}

		if truncated {
			event.Bool(logger.BodyCutLogKey, true)
		}

		if err != nil {
			event.Str(logger.RequestBodyReadErrKey, err.Error())
		}
	}
}

func ResponseLogFields() logger.EventFields {
	return func(
		ctx *operation.Context,
		event *zerolog.Event,
	) {
		state := ctx.State.(*State)
		resp := state.Writer

		event.
			Int(logger.ResponseStatusLogKey, resp.StatusCode()).
			Interface(logger.ResponseHeadersLogKey, loggableHeaders(resp.Header())).
			Str(logger.KindLogKey, KindResponse)

		if body := resp.Body(); len(body) > 0 {
			event.Str(
				logger.ResponseBodyLogKey,
				string(body),
			)
		}

		if resp.BodyTruncated() {
			event.Bool(
				logger.BodyCutLogKey,
				true,
			)
		}

	}
}

func loggableHeaders(headers http.Header) http.Header {
	result := headers.Clone()

	for name := range result {
		if _, sensitive := SensitiveHeaders[http.CanonicalHeaderKey(name)]; sensitive {
			result[name] = []string{"[*]"}
		}
	}

	return result
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
) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		bodyLimit:      bodyLimit,
	}
}

// responseRecorder observes a response without changing what is sent
// to the actual client.
//
// Only up to bodyLimit bytes are retained for logging.
type responseRecorder struct {
	http.ResponseWriter

	statusCode int

	body          []byte
	bodyLimit     int
	bodyTruncated bool
}

// Unwrap allows http.ResponseController to reach the original
// ResponseWriter.
func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	if r.statusCode != 0 {
		return
	}

	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if r.statusCode == 0 {
		// net/http implicitly sends 200 when Write is called before
		// WriteHeader.
		r.statusCode = http.StatusOK
	}

	r.capture(p)

	return r.ResponseWriter.Write(p)
}

func (r *responseRecorder) capture(p []byte) {
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

func (r *responseRecorder) StatusCode() int {
	return r.statusCode
}

func (r *responseRecorder) Body() []byte {
	return r.body
}

func (r *responseRecorder) BodyTruncated() bool {
	return r.bodyTruncated
}

type defaultHTTPErrorLog struct {
	l zerolog.Logger
}

func (w defaultHTTPErrorLog) Write(p []byte) (int, error) {
	l := len(p)
	if l > 0 && p[l-1] == '\n' {
		p = p[:l-1]
	}
	w.l.Error().Err(errors.New(string(p))).Msg("http server error")
	return l, nil
}
