// Package redact masks the values of sensitive fields before they are logged: in headers, JSON
// bodies and the errors that quote rejected input. Transports build a Fields set once, before
// serving, and skip masking entirely while it is empty.
package redact

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Mask replaces every sensitive value in logs.
const Mask = "[*]"

// errMaskedValue replaces an error that may quote a sensitive value.
var errMaskedValue = errors.New("invalid value " + Mask)

// Fields is a set of sensitive field names. Names match like strings.EqualFold, which is how
// encoding/json matches object keys to struct fields: "PASSWORD" and "paſſword" both match
// "password". The zero value is empty and matches nothing.
type Fields struct {
	names  map[string]struct{} // folded names
	maxLen int                 // length of the longest folded name
	// lengths has bit n set when a folded name is n bytes long, or bit 63 for any longer one.
	// Most names are rejected by length before their map lookup.
	lengths uint64
}

// NewFields returns the set of names. Empty names are ignored.
func NewFields(names ...string) Fields {
	var f Fields
	for _, name := range names {
		if name == "" {
			continue
		}
		if f.names == nil {
			f.names = make(map[string]struct{}, len(names))
		}
		folded, _ := fold(nil, name, math.MaxInt)
		f.names[string(folded)] = struct{}{}
		f.maxLen = max(f.maxLen, len(folded))
		f.lengths |= lengthBit(len(folded))
	}
	return f
}

// Empty reports whether no name is sensitive.
func (f Fields) Empty() bool {
	return len(f.names) == 0
}

// Has reports whether name is sensitive. It does not allocate while the set holds no name longer
// than 64 bytes.
func (f Fields) Has[S ~string | ~[]byte](name S) bool {
	if len(f.names) == 0 {
		return false
	}
	// Folding keeps the length of an ASCII name, so most are rejected without folding them.
	if f.lengths&lengthBit(len(name)) == 0 && isASCII(name) {
		return false
	}
	var buf [64]byte
	folded, ok := fold(buf[:0], name, f.maxLen)
	if !ok || f.lengths&lengthBit(len(folded)) == 0 {
		return false
	}
	_, found := f.names[string(folded)]
	return found
}

func lengthBit(n int) uint64 {
	return 1 << min(n, 63)
}

func isASCII[S ~string | ~[]byte](name S) bool {
	for i := range len(name) {
		if name[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// fold appends name to dst with every rune replaced by the smallest rune it equals under Unicode
// simple folding, so two names fold to the same bytes exactly when strings.EqualFold matches them.
// Folding never lengthens a name. fold gives up, reporting false, once the result exceeds limit.
func fold[S ~string | ~[]byte](dst []byte, name S, limit int) ([]byte, bool) {
	for i := 0; i < len(name); {
		if c := name[i]; c < utf8.RuneSelf {
			if 'a' <= c && c <= 'z' {
				c -= 'a' - 'A'
			}
			dst = append(dst, c)
			i++
		} else {
			// Converting at most utf8.UTFMax bytes keeps the conversion on the stack.
			r, size := utf8.DecodeRuneInString(string(name[i:min(i+utf8.UTFMax, len(name))]))
			dst = utf8.AppendRune(dst, foldRune(r))
			i += size
		}
		if len(dst) > limit {
			return dst, false
		}
	}
	return dst, true
}

// foldRune returns the smallest rune in the Unicode simple folding orbit of r.
func foldRune(r rune) rune {
	folded := r
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		folded = min(folded, f)
	}
	return folded
}

// MaskValueError returns err, the failure to parse a sensitive value, without that value. Parse
// errors tend to quote their input, as in `strconv.ParseInt: parsing "1234": invalid syntax`. A
// *strconv.NumError keeps its function and reason. Any other error is replaced, since there is no
// telling what its message quotes.
func MaskValueError(err error) error {
	if err == nil {
		return nil
	}
	var numErr *strconv.NumError
	if errors.As(err, &numErr) {
		return &strconv.NumError{Func: numErr.Func, Num: Mask, Err: numErr.Err}
	}
	return errMaskedValue
}

// MaskJSONError returns err without the input it may quote when it is a *json.UnmarshalTypeError
// about a sensitive field, or about anything nested in one. encoding/json quotes the numbers it
// rejects ("cannot unmarshal number 1e999 into ..."). The masked error replaces err as a whole,
// wrappers included, since their messages repeat the one they wrap.
func (f Fields) MaskJSONError(err error) error {
	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) || !f.hasPathSegment(typeErr.Field) {
		return err
	}
	kind, _, _ := strings.Cut(typeErr.Value, " ")
	return &json.UnmarshalTypeError{
		Value:  kind,
		Type:   typeErr.Type,
		Offset: typeErr.Offset,
		Struct: typeErr.Struct,
		Field:  typeErr.Field,
	}
}

// hasPathSegment reports whether a segment of a dot-separated JSON path, such as "card.pin" or
// "items.1.pin", is sensitive.
func (f Fields) hasPathSegment(path string) bool {
	for path != "" {
		var segment string
		segment, path, _ = strings.Cut(path, ".")
		if f.Has(segment) {
			return true
		}
	}
	return false
}
