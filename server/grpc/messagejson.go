package grpc

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"errors"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/efegreat/forgo/redact"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The JSON of messages in logs is what redact.Fields.LogJSON makes of protojson's, with proto field names:
// compact, with the value of every sensitive member masked at any depth. appendMessageJSON writes it in one
// pass, without the allocations of protojson and the second pass of the masker. TestMessageJSON and
// FuzzMessageJSON compare the two.

var (
	// errTooDeep is a message whose JSON nests deeper than redact.MaxJSONDepth, which LogJSON does not
	// follow. logMessage logs it the way it did before, through protojson and LogJSON.
	errTooDeep = errors.New("grpc: message too deep to log")
	// errNoJSON is a value protojson does not encode, such as a string that is not UTF-8, or a Timestamp
	// out of its range. The message is not logged, as protojson fails on it.
	errNoJSON = errors.New("grpc: message without a JSON encoding")
)

const (
	// maskedJSON replaces the value of a sensitive member, as redact masks it.
	maskedJSON = `"` + redact.Mask + `"`

	// The ranges of google.protobuf.Duration that protojson encodes.
	maxDurationSeconds = 315576000000
	maxNanos           = 999999999
)

// appendMessageJSON appends the JSON of m to dst, the values of fields masked. It fails where protojson
// fails, such as on a string that is not UTF-8 or a missing required field, and with errTooDeep on JSON
// that LogJSON would not follow. The well-known types with a JSON form of their own, other than wrappers,
// Timestamp and Duration, and the messages that can have extensions, are encoded by protojson, then
// compacted and masked by redact.
func appendMessageJSON(dst []byte, m proto.Message, fields redact.Fields) ([]byte, error) {
	if err := proto.CheckInitialized(m); err != nil {
		return dst, err
	}
	return appendMessage(dst, m.ProtoReflect(), fields, 1)
}

// appendMessage appends m, whose JSON object, if it has one, is level deep: 1 for the logged message.
func appendMessage(dst []byte, m protoreflect.Message, fields redact.Fields, level int) ([]byte, error) {
	md := m.Descriptor()
	if md.FullName().Parent() == "google.protobuf" {
		switch md.Name() {
		case "BoolValue", "Int32Value", "Int64Value", "UInt32Value", "UInt64Value",
			"FloatValue", "DoubleValue", "StringValue", "BytesValue":
			fd := md.Fields().ByNumber(1)
			return appendSingular(dst, m.Get(fd), fd, fields, level)
		case "Timestamp":
			return appendTimestamp(dst, m)
		case "Duration":
			return appendDuration(dst, m)
		case "Any", "Struct", "ListValue", "Value", "FieldMask":
			return appendProtoJSON(dst, m, fields, level)
		}
	}
	if md.ExtensionRanges().Len() > 0 {
		return appendProtoJSON(dst, m, fields, level)
	}
	if level > redact.MaxJSONDepth {
		return dst, errTooDeep
	}

	dst = append(dst, '{')
	fds := md.Fields()
	first := true
	for i := range fds.Len() {
		fd := fds.Get(i)
		if !m.Has(fd) {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		name := fd.TextName() // an identifier: nothing to escape
		dst = append(dst, '"')
		dst = append(dst, name...)
		dst = append(dst, '"', ':')

		// A sensitive value is encoded too, and dropped, so that it fails the message like in protojson.
		mark := len(dst)
		var err error
		v := m.Get(fd)
		switch {
		case fd.IsList():
			dst, err = appendList(dst, v.List(), fd, fields, level+1)
		case fd.IsMap():
			dst, err = appendMap(dst, v.Map(), fd, fields, level+1)
		default:
			dst, err = appendSingular(dst, v, fd, fields, level+1)
		}
		if err != nil {
			return dst, err
		}
		if fields.Has(name) {
			dst = append(dst[:mark], maskedJSON...)
		}
	}
	return append(dst, '}'), nil
}

func appendList(dst []byte, l protoreflect.List, fd protoreflect.FieldDescriptor, fields redact.Fields, level int) ([]byte, error) {
	if level > redact.MaxJSONDepth {
		return dst, errTooDeep
	}
	dst = append(dst, '[')
	for i := range l.Len() {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = appendSingular(dst, l.Get(i), fd, fields, level+1); err != nil {
			return dst, err
		}
	}
	return append(dst, ']'), nil
}

// appendMap appends the entries of mp ordered by key, as protojson does: false before true, numbers in
// ascending order, strings by bytes.
func appendMap(dst []byte, mp protoreflect.Map, fd protoreflect.FieldDescriptor, fields redact.Fields, level int) ([]byte, error) {
	if level > redact.MaxJSONDepth {
		return dst, errTooDeep
	}
	keys := make([]protoreflect.MapKey, 0, mp.Len())
	mp.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
		keys = append(keys, k)
		return true
	})
	kind := fd.MapKey().Kind()
	switch {
	case kind == protoreflect.BoolKind:
		slices.SortFunc(keys, func(x, y protoreflect.MapKey) int {
			return cmp.Compare(boolOrder(x.Bool()), boolOrder(y.Bool()))
		})
	case kind == protoreflect.StringKind:
		slices.SortFunc(keys, func(x, y protoreflect.MapKey) int { return cmp.Compare(x.String(), y.String()) })
	case isUnsignedKind(kind):
		slices.SortFunc(keys, func(x, y protoreflect.MapKey) int { return cmp.Compare(x.Uint(), y.Uint()) })
	default:
		slices.SortFunc(keys, func(x, y protoreflect.MapKey) int { return cmp.Compare(x.Int(), y.Int()) })
	}

	dst = append(dst, '{')
	valueFd := fd.MapValue()
	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}
		start := len(dst)
		var err error
		if dst, err = appendMapKey(dst, k, kind); err != nil {
			return dst, err
		}
		// The masker reads a key unescaped, which is the string key itself.
		var sensitive bool
		if kind == protoreflect.StringKind {
			sensitive = fields.Has(k.String())
		} else {
			sensitive = fields.Has(dst[start+1 : len(dst)-1])
		}
		dst = append(dst, ':')
		mark := len(dst)
		if dst, err = appendSingular(dst, mp.Get(k), valueFd, fields, level+1); err != nil {
			return dst, err
		}
		if sensitive {
			dst = append(dst[:mark], maskedJSON...)
		}
	}
	return append(dst, '}'), nil
}

func boolOrder(b bool) int {
	if b {
		return 1
	}
	return 0
}

func appendMapKey(dst []byte, k protoreflect.MapKey, kind protoreflect.Kind) ([]byte, error) {
	switch {
	case kind == protoreflect.StringKind:
		return appendJSONString(dst, k.String())
	case kind == protoreflect.BoolKind:
		dst = append(dst, '"')
		return append(strconv.AppendBool(dst, k.Bool()), '"'), nil
	case isUnsignedKind(kind):
		dst = append(dst, '"')
		return append(strconv.AppendUint(dst, k.Uint(), 10), '"'), nil
	default:
		dst = append(dst, '"')
		return append(strconv.AppendInt(dst, k.Int(), 10), '"'), nil
	}
}

func isUnsignedKind(kind protoreflect.Kind) bool {
	switch kind {
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return true
	}
	return false
}

// appendSingular appends a value that is not a list or a map. level is the depth of its JSON object, when
// it is a message.
func appendSingular(dst []byte, v protoreflect.Value, fd protoreflect.FieldDescriptor, fields redact.Fields, level int) ([]byte, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return strconv.AppendBool(dst, v.Bool()), nil
	case protoreflect.StringKind:
		return appendJSONString(dst, v.String())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return strconv.AppendInt(dst, v.Int(), 10), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return strconv.AppendUint(dst, v.Uint(), 10), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		// 64-bit integers are JSON strings.
		dst = append(dst, '"')
		return append(strconv.AppendInt(dst, v.Int(), 10), '"'), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		dst = append(dst, '"')
		return append(strconv.AppendUint(dst, v.Uint(), 10), '"'), nil
	case protoreflect.FloatKind:
		return appendJSONFloat(dst, v.Float(), 32), nil
	case protoreflect.DoubleKind:
		return appendJSONFloat(dst, v.Float(), 64), nil
	case protoreflect.BytesKind:
		dst = append(dst, '"')
		return append(base64.StdEncoding.AppendEncode(dst, v.Bytes()), '"'), nil
	case protoreflect.EnumKind:
		if fd.Enum().FullName() == "google.protobuf.NullValue" {
			return append(dst, "null"...), nil
		}
		if ev := fd.Enum().Values().ByNumber(v.Enum()); ev != nil {
			dst = append(dst, '"')
			return append(append(dst, ev.Name()...), '"'), nil // an identifier: nothing to escape
		}
		return strconv.AppendInt(dst, int64(v.Enum()), 10), nil
	default: // protoreflect.MessageKind, protoreflect.GroupKind
		return appendMessage(dst, v.Message(), fields, level)
	}
}

// appendTimestamp appends a google.protobuf.Timestamp as protojson does: RFC 3339 in UTC, with 0, 3, 6 or 9
// fractional digits.
func appendTimestamp(dst []byte, m protoreflect.Message) ([]byte, error) {
	fds := m.Descriptor().Fields()
	secs := m.Get(fds.ByNumber(1)).Int()
	nanos := m.Get(fds.ByNumber(2)).Int()
	if secs < minTimestampSeconds || secs > maxTimestampSeconds || nanos < 0 || nanos > maxNanos {
		return dst, errNoJSON
	}
	dst = append(dst, '"')
	start := len(dst)
	dst = time.Unix(secs, nanos).UTC().AppendFormat(dst, "2006-01-02T15:04:05.000000000")
	dst = trimFraction(dst, start)
	return append(dst, 'Z', '"'), nil
}

// appendDuration appends a google.protobuf.Duration as protojson does: seconds, with 0, 3, 6 or 9
// fractional digits, and the suffix "s".
func appendDuration(dst []byte, m protoreflect.Message) ([]byte, error) {
	fds := m.Descriptor().Fields()
	secs := m.Get(fds.ByNumber(1)).Int()
	nanos := m.Get(fds.ByNumber(2)).Int()
	if secs < -maxDurationSeconds || secs > maxDurationSeconds || nanos < -maxNanos || nanos > maxNanos ||
		(secs > 0 && nanos < 0) || (secs < 0 && nanos > 0) {
		return dst, errNoJSON
	}
	dst = append(dst, '"')
	start := len(dst)
	if secs < 0 || nanos < 0 {
		dst = append(dst, '-')
		secs, nanos = -secs, -nanos
	}
	dst = strconv.AppendInt(dst, secs, 10)
	var frac [10]byte // ".%09d"
	frac[0] = '.'
	for i := 9; i > 0; i-- {
		frac[i] = byte('0' + nanos%10)
		nanos /= 10
	}
	dst = trimFraction(append(dst, frac[:]...), start)
	return append(dst, 's', '"'), nil
}

// trimFraction drops trailing groups of three zeros from the fraction that ends dst[start:], and the
// fraction itself when it is all zeros, like protojson's strings.TrimSuffix calls.
func trimFraction(dst []byte, start int) []byte {
	for range 2 {
		if bytes.HasSuffix(dst[start:], []byte("000")) {
			dst = dst[:len(dst)-3]
		}
	}
	if bytes.HasSuffix(dst[start:], []byte(".000")) {
		dst = dst[:len(dst)-4]
	}
	return dst
}

// appendProtoJSON appends m encoded by protojson, compacted and masked by redact. level is the depth of its
// outermost JSON object or array.
func appendProtoJSON(dst []byte, m protoreflect.Message, fields redact.Fields, level int) ([]byte, error) {
	bufp := bufferPool.Get().(*[]byte)
	raw, err := messageJSON.MarshalAppend((*bufp)[:0], m.Interface())
	if err == nil {
		var valid bool
		if level-1+jsonDepth(raw) > redact.MaxJSONDepth {
			err = errTooDeep
		} else if dst, valid = fields.AppendJSON(dst, raw); !valid {
			err = errNoJSON
		}
	}
	if cap(raw) <= maxPooledBuffer {
		*bufp = raw[:0]
		bufferPool.Put(bufp)
	}
	return dst, err
}

// jsonDepth returns how deep the objects and arrays of the valid JSON b nest.
func jsonDepth(b []byte) int {
	depth, deepest := 0, 0
	inString := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inString {
			switch c {
			case '\\':
				i++
			case '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			deepest = max(deepest, depth)
		case '}', ']':
			depth--
		}
	}
	return deepest
}

// appendJSONString appends s as a JSON string, escaped like protojson does, and fails on invalid UTF-8.
func appendJSONString(dst []byte, s string) ([]byte, error) {
	dst = append(dst, '"')
	i := indexNeedEscape(s)
	dst = append(dst, s[:i]...)
	s = s[i:]
	for len(s) > 0 {
		r, n := utf8.DecodeRuneInString(s)
		switch {
		case r == utf8.RuneError && n == 1:
			return dst, errNoJSON
		case r < ' ' || r == '"' || r == '\\':
			dst = append(dst, '\\')
			switch r {
			case '"', '\\':
				dst = append(dst, byte(r))
			case '\b':
				dst = append(dst, 'b')
			case '\f':
				dst = append(dst, 'f')
			case '\n':
				dst = append(dst, 'n')
			case '\r':
				dst = append(dst, 'r')
			case '\t':
				dst = append(dst, 't')
			default:
				dst = append(dst, 'u')
				dst = append(dst, "0000"[1+(bits.Len32(uint32(r))-1)/4:]...)
				dst = strconv.AppendUint(dst, uint64(r), 16)
			}
			s = s[n:]
		default:
			i := indexNeedEscape(s[n:])
			dst = append(dst, s[:n+i]...)
			s = s[n+i:]
		}
	}
	return append(dst, '"'), nil
}

// indexNeedEscape returns the index of the first rune of s that appendJSONString escapes or checks.
func indexNeedEscape(s string) int {
	for i, r := range s {
		if r < ' ' || r == '\\' || r == '"' || r == utf8.RuneError {
			return i
		}
	}
	return len(s)
}

// appendJSONFloat appends n as protojson does: like encoding/json, with NaN and the infinities as strings.
func appendJSONFloat(dst []byte, n float64, bitSize int) []byte {
	switch {
	case math.IsNaN(n):
		return append(dst, `"NaN"`...)
	case math.IsInf(n, 1):
		return append(dst, `"Infinity"`...)
	case math.IsInf(n, -1):
		return append(dst, `"-Infinity"`...)
	}
	format := byte('f')
	if abs := math.Abs(n); abs != 0 {
		if bitSize == 64 && (abs < 1e-6 || abs >= 1e21) ||
			bitSize == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	dst = strconv.AppendFloat(dst, n, format, -1, bitSize)
	if format == 'e' {
		// clean up e-09 to e-9
		if n := len(dst); n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst
}
