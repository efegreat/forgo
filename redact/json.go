package redact

import (
	"sync"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/efegreat/forgo/logger"

	"github.com/rs/zerolog"
)

// MaxJSONDepth bounds the nesting of objects and arrays that LogJSON and AppendJSON follow, so a hostile
// body cannot grow the stack without bound. Deeper JSON is treated as invalid. Encoders that write
// compact, masked JSON themselves, such as the gRPC server's for messages, read it to log what LogJSON
// would.
const MaxJSONDepth = 1000

const (
	// maskedJSON replaces the value of a sensitive member.
	maskedJSON = `"` + Mask + `"`
	// maxPooledBuffer keeps a single huge body from pinning its buffer in the pool.
	maxPooledBuffer = 64 * 1024
)

// bufferPool holds the buffers masked bodies are written into. A buffer never outlives LogJSON:
// the event copies it before it goes back to the pool.
var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 1024)
		return &buf
	},
}

// LogJSON adds body to event under key. The caller cut body at its logging limit when truncated
// is set.
//
// A whole, valid body is logged as compact JSON: whitespace between tokens is dropped, since a
// newline would split the log line, and the value of every sensitive member is masked at any depth.
// Any other body is logged as a string:
//   - Without sensitive fields, it is logged whole, with logger.BodyCutLogKey set when truncated.
//   - With them, it is logged up to where it stops being valid JSON, with logger.BodyCutLogKey set.
//     The rest is dropped, since it cannot be masked, so a body that is not JSON logs as "".
func (f Fields) LogJSON(event *zerolog.Event, key string, body []byte, truncated bool) {
	if len(body) == 0 || !event.Enabled() {
		return
	}
	if truncated && f.Empty() {
		event.Bytes(key, body) // nothing to mask
		event.Bool(logger.BodyCutLogKey, true)
		return
	}

	bufp := bufferPool.Get().(*[]byte)
	m := jsonMasker[[]byte]{fields: f, src: body, out: (*bufp)[:0]}
	valid := m.mask()
	masked := output(&m)
	switch {
	case valid && !truncated:
		event.RawJSON(key, masked)
	case f.Empty():
		event.Bytes(key, body) // not JSON, and nothing to mask
	default:
		event.Bytes(key, masked)
		event.Bool(logger.BodyCutLogKey, true)
	}

	if cap(m.out) <= maxPooledBuffer {
		*bufp = m.out
		bufferPool.Put(bufp)
	}
}

// AppendJSON appends body to dst the way LogJSON logs a valid body, compacted and with the value of
// every sensitive member masked at any depth, and reports whether body is one whole, valid JSON value.
// When it is not, AppendJSON stops at the first byte that is not valid JSON, so what it appended
// covers body only up to there, without the partial value of a sensitive member. A value cut at a
// logging limit is therefore never valid, and its sensitive values stay masked. AppendJSON does not
// allocate when dst has room, and reads a string body without copying it.
func (f Fields) AppendJSON[S ~string | ~[]byte](dst []byte, body S) (out []byte, valid bool) {
	m := jsonMasker[S]{fields: f, src: body, out: dst}
	valid = m.mask()
	if m.flushed == 0 {
		return append(dst, m.src[:m.pos]...), valid
	}
	return m.out, valid
}

// jsonMasker copies JSON compacted, replacing the value of every sensitive object member with the
// mask. It is a recursive descent parser that also validates, so masked output is only trusted as
// JSON when the whole input was.
type jsonMasker[S ~string | ~[]byte] struct {
	fields Fields
	src    S
	pos    int // next byte of src to scan
	// out holds the output for src[:flushed], and src[flushed:pos] still has to be appended.
	// flushed stays 0 until a value is masked or whitespace dropped, so compact JSON with nothing
	// to mask is never copied.
	out     []byte
	flushed int
	// inMask is set while scanning a masked value, which is dropped whole.
	inMask bool
}

// mask compacts src and masks its sensitive values. The result is out, or src[:pos] itself while
// flushed is 0, when that changes nothing. It stops at the first byte that is not valid JSON, so the
// result covers src only up to there, without the partial value of a sensitive member, and valid is
// false.
func (m *jsonMasker[S]) mask() (valid bool) {
	m.space()
	valid = m.value(0)
	if valid {
		m.space()
		valid = m.pos == len(m.src)
	}
	if m.flushed > 0 {
		m.out = append(m.out, m.src[m.flushed:m.pos]...)
	}
	return valid
}

// output returns the result of mask over bytes, which is src itself when nothing changed.
func output(m *jsonMasker[[]byte]) []byte {
	if m.flushed == 0 {
		return m.src[:m.pos]
	}
	return m.out
}

func (m *jsonMasker[S]) value(depth int) bool {
	if m.pos == len(m.src) {
		return false
	}
	switch m.src[m.pos] {
	case '{':
		return m.object(depth + 1)
	case '[':
		return m.array(depth + 1)
	case '"':
		_, _, ok := m.string()
		return ok
	case 't':
		return m.literal("true")
	case 'f':
		return m.literal("false")
	case 'n':
		return m.literal("null")
	default:
		return m.number()
	}
}

func (m *jsonMasker[S]) object(depth int) bool {
	if depth > MaxJSONDepth {
		return false
	}
	m.pos++ // {
	m.space()
	if m.next('}') {
		return true
	}
	for {
		if !m.member(depth) {
			return false
		}
		m.space()
		if m.next('}') {
			return true
		}
		if !m.next(',') {
			return false
		}
		m.space()
	}
}

// member scans one "key": value pair of an object, masking the value when the key is sensitive.
func (m *jsonMasker[S]) member(depth int) bool {
	if m.pos == len(m.src) || m.src[m.pos] != '"' {
		return false
	}
	key, escaped, ok := m.string()
	if !ok {
		return false
	}
	m.space()
	if !m.next(':') {
		return false
	}
	m.space()
	if m.inMask || !m.fields.hasKey(key, escaped) {
		return m.value(depth)
	}

	m.out = append(m.out, m.src[m.flushed:m.pos]...)
	m.out = append(m.out, maskedJSON...)
	m.inMask = true
	ok = m.value(depth)
	m.inMask = false
	m.flushed = m.pos // the value is dropped, even a partial one
	return ok
}

func (m *jsonMasker[S]) array(depth int) bool {
	if depth > MaxJSONDepth {
		return false
	}
	m.pos++ // [
	m.space()
	if m.next(']') {
		return true
	}
	for {
		if !m.value(depth) {
			return false
		}
		m.space()
		if m.next(']') {
			return true
		}
		if !m.next(',') {
			return false
		}
		m.space()
	}
}

// string scans a string and returns its raw contents, between the quotes. escaped reports whether
// they hold escape sequences.
func (m *jsonMasker[S]) string() (raw S, escaped, ok bool) {
	start := m.pos + 1
	for i := start; i < len(m.src); i++ {
		switch c := m.src[i]; {
		case c == '"':
			m.pos = i + 1
			return m.src[start:i], escaped, true
		case c == '\\':
			n := escapeLen(m.src[i:])
			if n == 0 {
				m.pos = i
				return raw, false, false
			}
			escaped = true
			i += n - 1
		case c < 0x20:
			m.pos = i
			return raw, false, false
		}
	}
	m.pos = len(m.src)
	return raw, false, false
}

// escapeLen returns the length of the escape sequence s starts with, or 0 when it is invalid or cut.
func escapeLen[S ~string | ~[]byte](s S) int {
	if len(s) < 2 {
		return 0
	}
	switch s[1] {
	case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
		return 2
	case 'u':
		if len(s) >= 6 && isHex(s[2]) && isHex(s[3]) && isHex(s[4]) && isHex(s[5]) {
			return 6
		}
	}
	return 0
}

// number scans -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
func (m *jsonMasker[S]) number() bool {
	m.next('-')
	switch {
	case m.next('0'):
	case m.digits():
	default:
		return false
	}
	if m.next('.') && !m.digits() {
		return false
	}
	if m.next('e') || m.next('E') {
		if !m.next('+') {
			m.next('-')
		}
		if !m.digits() {
			return false
		}
	}
	return true
}

// digits scans one or more digits.
func (m *jsonMasker[S]) digits() bool {
	start := m.pos
	for m.pos < len(m.src) && '0' <= m.src[m.pos] && m.src[m.pos] <= '9' {
		m.pos++
	}
	return m.pos > start
}

func (m *jsonMasker[S]) literal(lit string) bool {
	end := m.pos + len(lit)
	if end > len(m.src) || string(m.src[m.pos:end]) != lit {
		return false
	}
	m.pos = end
	return true
}

// space skips whitespace and drops it from the output, unless it is part of a masked value, which
// is dropped whole.
func (m *jsonMasker[S]) space() {
	start := m.pos
	for m.pos < len(m.src) && isSpace(m.src[m.pos]) {
		m.pos++
	}
	if m.pos > start && !m.inMask {
		m.out = append(m.out, m.src[m.flushed:start]...)
		m.flushed = m.pos
	}
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// next consumes c when it is the next byte.
func (m *jsonMasker[S]) next(c byte) bool {
	if m.pos < len(m.src) && m.src[m.pos] == c {
		m.pos++
		return true
	}
	return false
}

// hasKey reports whether the raw contents of an object key name a sensitive field. An escaped key
// is decoded first, since "pass\u0077ord" sets a password field too.
func (f Fields) hasKey[S ~string | ~[]byte](raw S, escaped bool) bool {
	if !escaped {
		return f.Has(raw)
	}
	var buf [64]byte
	return f.Has(unescape(buf[:0], raw))
}

// unescape appends the decoded contents of a JSON string to dst. raw must be valid, as scanned
// by jsonMasker.string. Invalid surrogates decode to U+FFFD, like encoding/json does.
func unescape[S ~string | ~[]byte](dst []byte, raw S) []byte {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			dst = append(dst, raw[i])
			continue
		}
		i++
		switch raw[i] {
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			r := hexRune(raw[i+1 : i+5])
			i += 4
			// A rune outside the BMP is a pair of escaped surrogates: \ud83d\ude00.
			if utf16.IsSurrogate(r) && i+6 < len(raw) && raw[i+1] == '\\' && raw[i+2] == 'u' {
				if pair := utf16.DecodeRune(r, hexRune(raw[i+3:i+7])); pair != utf8.RuneError {
					r = pair
					i += 6
				}
			}
			dst = utf8.AppendRune(dst, r)
		default: // '"', '\\', '/'
			dst = append(dst, raw[i])
		}
	}
	return dst
}

// hexRune decodes four hex digits.
func hexRune[S ~string | ~[]byte](hex S) rune {
	var r rune
	for i := range len(hex) {
		c := hex[i]
		switch {
		case c <= '9':
			c -= '0'
		case c <= 'F':
			c -= 'A' - 10
		default:
			c -= 'a' - 10
		}
		r = r<<4 | rune(c)
	}
	return r
}

func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
}
