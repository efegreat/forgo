package redact

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestFieldsHas(t *testing.T) {
	fields := NewFields("password", "X-Api-Key", "", "clé")
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"password", true},
		{"PASSWORD", true},
		{"PassWord", true},
		{"pa\u017f\u017fword", true}, // ſ (long s) folds to s, like encoding/json matches it
		{"x-api-key", true},
		{"X-API-\u212Aey", true}, // Kelvin sign folds to k
		{"CLÉ", true},
		{"pass", false},
		{"passwords", false},
		{"password ", false},
		{"pass_word", false},
		{"", false},
		{"\u00e9", false}, // not ASCII, and no name folds to its length
		{strings.Repeat("password", 100), false},
		{strings.Repeat("pa\u017f\u017fword", 100), false}, // not ASCII, and longer than every name once folded
	} {
		if got := fields.Has(tc.name); got != tc.want {
			t.Errorf("Has(%q) = %v, want %v", tc.name, got, tc.want)
		}
		if got := fields.Has([]byte(tc.name)); got != tc.want {
			t.Errorf("Has([]byte(%q)) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestEmptyFields(t *testing.T) {
	for _, fields := range []Fields{{}, NewFields(), NewFields("", "")} {
		if !fields.Empty() || fields.Has("") || fields.Has("password") {
			t.Fatalf("%+v is not empty", fields)
		}
	}
	if NewFields("password").Empty() {
		t.Fatal("a set with a name is empty")
	}
}

// FuzzFieldsHas checks that names match exactly like strings.EqualFold, which is how encoding/json
// matches object keys to struct fields.
func FuzzFieldsHas(f *testing.F) {
	f.Add("password", "PASSWORD")
	f.Add("password", "pa\u017f\u017fword")
	f.Add("key", "\u212Aey")
	f.Add("straße", "STRASSE")
	f.Add("\xff", "\xfe")
	f.Fuzz(func(t *testing.T, name, candidate string) {
		want := name != "" && strings.EqualFold(name, candidate)
		if got := NewFields(name).Has(candidate); got != want {
			t.Fatalf("NewFields(%q).Has(%q) = %v, strings.EqualFold says %v", name, candidate, got, want)
		}
	})
}

func TestHasDoesNotAllocate(t *testing.T) {
	fields := NewFields("password", "authorization", "x-api-key", strings.Repeat("a", 64))
	key := []byte("Pa\u017f\u017fword")
	long := strings.Repeat("x", 200)
	// Folds past the longest name by a whole 4-byte rune before Has gives up.
	overshoot := strings.Repeat("a", 63) + "\U0001F600"
	allocs := testing.AllocsPerRun(100, func() {
		fields.Has("Authorization")
		fields.Has("Content-Type")
		fields.Has(key)
		fields.Has(long)
		fields.Has(overshoot)
	})
	if allocs != 0 {
		t.Fatalf("Has allocated %v times", allocs)
	}
}

func TestMaskValueError(t *testing.T) {
	_, numErr := strconv.ParseInt("sk_live_123", 10, 64)
	wrapped := fmt.Errorf("parsing sk_live_123: %w", numErr)

	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"number", numErr, `strconv.ParseInt: parsing "[*]": invalid syntax`},
		{"wrapped number", wrapped, `strconv.ParseInt: parsing "[*]": invalid syntax`},
		{"other", errors.New("bad value sk_live_123"), "invalid value [*]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			masked := MaskValueError(tc.err)
			if masked.Error() != tc.want {
				t.Fatalf("masked %q, want %q", masked, tc.want)
			}
		})
	}
	if !errors.Is(MaskValueError(numErr), strconv.ErrSyntax) {
		t.Fatal("masked number error lost its reason")
	}
	if MaskValueError(nil) != nil {
		t.Fatal("masked a nil error")
	}
}

func TestMaskJSONError(t *testing.T) {
	type card struct {
		Pin int8 `json:"pin"`
		Day int8 `json:"day"`
	}
	type request struct {
		Card   card            `json:"card"`
		Items  []card          `json:"items"`
		Secret map[string]int8 `json:"secret"`
		Amount int8            `json:"amount"`
		PinStr int             `json:"pin_str,string"`
	}
	unmarshalErr := func(body string) error {
		var req request
		err := json.Unmarshal([]byte(body), &req)
		if err == nil {
			t.Fatalf("%s: no error", body)
		}
		return err
	}
	fields := NewFields("pin", "secret", "pin_str")

	for _, tc := range []struct {
		body   string
		masked bool
	}{
		{`{"card":{"pin":1000}}`, true},
		{`{"items":[{"day":1},{"PIN":1000}]}`, true},
		{`{"secret":{"day":1000}}`, true}, // nested in a sensitive field
		// The ,string option quotes whatever string the field held, not only a number.
		{`{"pin_str":"1000_sk_live"}`, true},
		{`{"card":{"day":1000}}`, false},
		{`{"amount":1000}`, false},
	} {
		err := unmarshalErr(tc.body)
		if !strings.Contains(err.Error(), "1000") {
			t.Fatalf("%s: error %q does not quote the input", tc.body, err)
		}
		masked := fields.MaskJSONError(err)
		leaked := strings.Contains(masked.Error(), "1000")
		if leaked == tc.masked {
			t.Errorf("%s: masked error %q, want masked=%v", tc.body, masked, tc.masked)
		}
		var typeErr *json.UnmarshalTypeError
		if tc.masked && (!errors.As(masked, &typeErr) || typeErr.Value != "number") {
			t.Errorf("%s: masked error %#v is not a number type error", tc.body, masked)
		}
		if !tc.masked && masked != err {
			t.Errorf("%s: error replaced although nothing is masked", tc.body)
		}
	}

	wrapped := fmt.Errorf("bind: %w", unmarshalErr(`{"card":{"pin":1000}}`))
	if masked := fields.MaskJSONError(wrapped); strings.Contains(masked.Error(), "1000") {
		t.Errorf("wrapped error leaked: %q", masked)
	}
	syntaxErr := unmarshalErr(`{"pin":`)
	if fields.MaskJSONError(syntaxErr) != syntaxErr {
		t.Error("an error that is no type error was replaced")
	}
	if NewFields().MaskJSONError(nil) != nil {
		t.Error("masked a nil error")
	}
}
