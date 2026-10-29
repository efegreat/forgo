// Package headerlog logs header maps as JSON without reflection, byte for byte like an encoding/json
// Encoder without HTML escaping.
package headerlog

import (
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/efegreat/forgo/logger"

	"github.com/rs/zerolog"
)

// maxPooledBuffer keeps a single huge header from pinning its buffer in the pool.
const maxPooledBuffer = 64 * 1024

// bufferPool holds the buffers headers are encoded into. A buffer never outlives Log:
// the event copies it before it goes back to the pool.
var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 1024)
		return &buf
	},
}

// Header is one header to log. Transports collect them into a stack array, so typical header
// counts never reach the heap.
type Header[V any] struct {
	Name  string
	Value V
}

// ValueAppender appends the JSON of one header value. Transports mask sensitive values in it.
type ValueAppender[V any] func(buf []byte, name string, value V) []byte

// Log adds headers to event under key as JSON. Past limit bytes it logs the cut JSON as a string
// and sets logger.HeadersCutLogKey. A negative limit counts as 0. It sorts headers in place, and
// does nothing for a disabled event.
func Log[V any](event *zerolog.Event, key string, headers []Header[V], limit int, appendValue ValueAppender[V]) {
	if !event.Enabled() {
		return
	}
	limit = max(limit, 0)

	bufp := bufferPool.Get().(*[]byte)
	buf := Append((*bufp)[:0], headers, limit, appendValue)

	if len(buf) <= limit {
		event.RawJSON(key, buf)
	} else {
		event.Str(key, string(buf[:limit]))
		event.Bool(logger.HeadersCutLogKey, true)
	}

	if cap(buf) <= maxPooledBuffer {
		*bufp = buf
		bufferPool.Put(bufp)
	}
}

// Append appends headers to buf as an encoding/json Encoder without HTML escaping encodes the
// equivalent map: names sorted, nil
// headers as null. It sorts headers in place and stops once buf is longer than limit, since only the
// first limit bytes are logged then.
func Append[V any](buf []byte, headers []Header[V], limit int, appendValue ValueAppender[V]) []byte {
	if headers == nil {
		return append(buf, "null"...)
	}

	slices.SortFunc(headers, func(a, b Header[V]) int {
		return strings.Compare(a.Name, b.Name)
	})

	buf = append(buf, '{')
	for i, header := range headers {
		if len(buf) > limit {
			return buf
		}
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = AppendString(buf, header.Name)
		buf = append(buf, ':')
		buf = appendValue(buf, header.Name, header.Value)
	}
	return append(buf, '}')
}

// AppendString appends s as a JSON string escaped like an encoding/json Encoder with
// SetEscapeHTML(false): quotes, backslashes, control characters and the U+2028/U+2029 line separators
// are escaped, invalid UTF-8 becomes U+FFFD, and <, > and & are kept as they are, like zerolog keeps
// them in every other field. A Link header thus reads <https://…>; rel="next" in the log.
func AppendString(buf []byte, s string) []byte {
	const hex = "0123456789abcdef"

	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		if c := s[i]; c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			buf = append(buf, s[start:i]...)
			switch c {
			case '"', '\\':
				buf = append(buf, '\\', c)
			case '\b':
				buf = append(buf, '\\', 'b')
			case '\f':
				buf = append(buf, '\\', 'f')
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				buf = append(buf, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			}
			i++
			start = i
			continue
		}

		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			buf = append(buf, s[start:i]...)
			buf = append(buf, "\ufffd"...)
		case r == '\u2028' || r == '\u2029':
			buf = append(buf, s[start:i]...)
			buf = append(buf, '\\', 'u', '2', '0', '2', hex[r&0xf])
		default:
			i += size
			continue
		}
		i += size
		start = i
	}
	buf = append(buf, s[start:]...)
	return append(buf, '"')
}
