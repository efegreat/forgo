package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/efegreat/forgo/redact"

	"github.com/go-playground/validator/v10"
)

// IBinder decodes the payload of a message into dst, a pointer to the handler's request, and validates
// it. An error rejects the message for good, with code msg_bind.
type IBinder interface {
	Bind(Message, any) error
}

// timeType is the struct type that go-playground/validator does not validate as a struct.
var timeType = reflect.TypeFor[time.Time]()

type defaultBinder struct {
	validator *validator.Validate
}

// Bind decodes the payload as JSON, keeping the numbers of interface values as json.Number, then validates
// a struct request with its validate tags. An empty payload is not decoded, so the request keeps its zero
// value and still fails tags such as required. Requests of other kinds, such as slices, maps or
// json.RawMessage, are decoded but not validated.
func (db *defaultBinder) Bind(m Message, dst any) error {
	if len(m.Payload) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(m.Payload))
		decoder.UseNumber()

		if err := decoder.Decode(dst); err != nil {
			return err
		}
	}

	if s := structOf(dst); s != nil {
		return db.validator.Struct(s)
	}
	return nil
}

// structOf returns a pointer to the struct that dst points to, through any number of pointers, or nil when
// dst points to another kind or to a nil pointer. validator.Struct fails on anything else.
func structOf(dst any) any {
	v := reflect.ValueOf(dst)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct || v.Type().ConvertibleTo(timeType) || !v.CanAddr() {
		return nil
	}
	return v.Addr().Interface()
}

// newDefaultBinder returns the default binder, with validators registered as custom tags.
func newDefaultBinder(validators map[string]validator.Func) (IBinder, error) {
	val := validator.New(validator.WithRequiredStructEnabled())
	for _, name := range slices.Sorted(maps.Keys(validators)) {
		// validator returns an error for an empty name or a nil function, and panics on a name it reserves,
		// such as "dive" or one with a comma.
		if err := val.RegisterValidation(name, validators[name]); err != nil {
			return nil, fmt.Errorf("worker: validator %q: %w", name, err)
		}
	}
	return &defaultBinder{validator: val}, nil
}

// maskingBinder masks sensitive input in the errors of the binder it wraps: encoding/json quotes
// the numbers it rejects in a *json.UnmarshalTypeError.
type maskingBinder struct {
	IBinder
	sensitive redact.Fields
}

func (b maskingBinder) Bind(m Message, dst any) error {
	if err := b.IBinder.Bind(m, dst); err != nil {
		return b.sensitive.MaskJSONError(err)
	}
	return nil
}
