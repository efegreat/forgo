package http

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/efegreat/forgo/internal/headerlog"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/redact"

	"github.com/efegreat/bingo"
	"github.com/rs/zerolog"
)

var maskedHeaderValues = []string{redact.Mask}

// sensitiveFields holds the names whose values are masked in logs.
type sensitiveFields struct {
	// headers are masked in request and response headers: the configured names and
	// redact.CredentialHeaders.
	headers redact.Fields
	// fields are masked everywhere else: in the path, query, bodies and binding errors.
	fields redact.Fields
}

func newSensitiveFields(names []string) sensitiveFields {
	return sensitiveFields{
		headers: redact.NewFields(slices.Concat(redact.CredentialHeaders, names)...),
		fields:  redact.NewFields(names...),
	}
}

// headerValuesAppender returns the headerlog.ValueAppender that writes the values of one header as
// a JSON array, masking the values of sensitive headers.
func (s sensitiveFields) headerValuesAppender() headerlog.ValueAppender[[]string] {
	return func(buf []byte, name string, values []string) []byte {
		if s.headers.Has(name) {
			values = maskedHeaderValues
		}
		if values == nil {
			return append(buf, "null"...)
		}

		buf = append(buf, '[')
		for i, value := range values {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = headerlog.AppendString(buf, value)
		}
		return append(buf, ']')
	}
}

// logPath logs the request path with the values of the sensitive wildcards of its route masked.
func (s sensitiveFields) logPath(event *zerolog.Event, r *http.Request) {
	if !s.fields.Empty() {
		var buf [512]byte
		if masked, ok := maskPath(buf[:0], r, s.fields); ok {
			event.Bytes(logger.RequestPathLogKey, masked)
			return
		}
	}
	event.Str(logger.RequestPathLogKey, r.URL.Path)
}

// logQuery logs the raw query with the values of sensitive parameters masked.
func (s sensitiveFields) logQuery(event *zerolog.Event, query string) {
	if !s.fields.Empty() {
		var buf [512]byte
		if masked, ok := maskQuery(buf[:0], query, s.fields); ok {
			event.Bytes(logger.RequestQueryLogKey, masked)
			return
		}
	}
	event.Str(logger.RequestQueryLogKey, query)
}

// maskPath appends the path of r to dst with the values of the sensitive wildcards of its route
// pattern masked, a {name...} wildcard masking the rest of the path. It reports false, leaving dst
// unchanged, when the pattern has no sensitive wildcard.
func maskPath(dst []byte, r *http.Request, fields redact.Fields) ([]byte, bool) {
	start := strings.IndexByte(r.Pattern, '/') // skip the method and host
	if start < 0 || !hasSensitiveWildcard(r.Pattern[start:], fields) {
		return dst, false
	}

	// Wildcards match the segments of the escaped path, where a slash inside a value is still escaped.
	pattern, path := r.Pattern[start:], r.URL.EscapedPath()
	for pattern != "" && path != "" {
		segment, patternRest := cutSegment(pattern)
		value, pathRest := cutSegment(path)
		name, rest := wildcard(segment)

		dst = append(dst, '/')
		switch {
		case name != "" && fields.Has(name):
			dst = append(dst, redact.Mask...)
			if rest {
				return dst, true
			}
		case rest:
			return appendUnescapedPath(dst, path[1:]), true
		default:
			dst = appendUnescapedPath(dst, value)
		}
		pattern, path = patternRest, pathRest
	}
	// What remains was matched by a pattern ending in a slash.
	return appendUnescapedPath(dst, path), true
}

func hasSensitiveWildcard(pattern string, fields redact.Fields) bool {
	for pattern != "" {
		var segment string
		segment, pattern = cutSegment(pattern)
		if name, _ := wildcard(segment); name != "" && fields.Has(name) {
			return true
		}
	}
	return false
}

// cutSegment cuts the first segment off a path that starts with a slash.
func cutSegment(path string) (segment, rest string) {
	segment = path[1:]
	if i := strings.IndexByte(segment, '/'); i >= 0 {
		return segment[:i], segment[i:]
	}
	return segment, ""
}

// wildcard returns the name of a {name} or {name...} pattern segment, and whether it matches the
// rest of the path.
func wildcard(segment string) (name string, rest bool) {
	name, ok := strings.CutPrefix(segment, "{")
	if !ok {
		return "", false
	}
	if name, ok = strings.CutSuffix(name, "}"); !ok {
		return "", false
	}
	name, rest = strings.CutSuffix(name, "...")
	return name, rest
}

// appendUnescapedPath appends an escaped path to dst the way it reads in URL.Path.
func appendUnescapedPath(dst []byte, path string) []byte {
	if unescaped, err := url.PathUnescape(path); err == nil {
		path = unescaped
	}
	return append(dst, path...)
}

// maskQuery appends query to dst with the values of sensitive parameters masked. It reports false,
// leaving dst unchanged, when no parameter is sensitive.
//
// Names are unescaped before matching, as bingo does. Pairs are split on the legacy ";" separator
// too: bingo skips a pair holding one, but its value would still be logged.
func maskQuery(dst []byte, query string, fields redact.Fields) ([]byte, bool) {
	masked, flushed := false, 0
	for start := 0; start < len(query); {
		end := strings.IndexAny(query[start:], "&;")
		if end < 0 {
			end = len(query)
		} else {
			end += start
		}

		name, _, hasValue := strings.Cut(query[start:end], "=")
		if hasValue && isSensitiveParam(name, fields) {
			valueStart := start + len(name) + 1
			dst = append(dst, query[flushed:valueStart]...)
			dst = append(dst, redact.Mask...)
			flushed, masked = end, true
		}
		start = end + 1
	}
	if !masked {
		return dst, false
	}
	return append(dst, query[flushed:]...), true
}

func isSensitiveParam(name string, fields redact.Fields) bool {
	if unescaped, err := url.QueryUnescape(name); err == nil {
		name = unescaped
	}
	return fields.Has(name)
}

// maskingBinder masks sensitive input in the errors of the binder it wraps.
type maskingBinder struct {
	IBinder
	sensitive sensitiveFields
}

func (b maskingBinder) Bind(r *http.Request, dst any) error {
	if err := b.IBinder.Bind(r, dst); err != nil {
		return b.sensitive.maskBindError(err)
	}
	return nil
}

// maskBindError masks the input that binding errors carry for sensitive fields. bingo keeps input
// out of its own messages, but the Cause of a FieldError may quote it, and that cause is what
// MapBinding wraps and the Recorder logs. Body errors are encoding/json errors.
func (s sensitiveFields) maskBindError(err error) error {
	switch e := err.(type) {
	case bingo.Errors:
		masked := make(bingo.Errors, len(e))
		for i, fieldErr := range e {
			masked[i] = s.maskFieldError(fieldErr)
		}
		return masked
	case bingo.FieldError:
		return s.maskFieldError(e)
	case bingo.BodyError:
		e.Cause = s.fields.MaskJSONError(e.Cause)
		return e
	default:
		return s.fields.MaskJSONError(err)
	}
}

func (s sensitiveFields) maskFieldError(e bingo.FieldError) bingo.FieldError {
	names := s.fields
	if e.Source == bingo.SourceHeader {
		names = s.headers
	}
	if names.Has(e.Name) {
		e.Cause = redact.MaskValueError(e.Cause)
	}
	return e
}
