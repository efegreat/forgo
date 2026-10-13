package http

import (
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"github.com/efegreat/bingo"
	"github.com/go-playground/validator/v10"
)

// IBinder binds a request to dst, a pointer to the handler's request, and validates it. The server's
// ErrorMapper maps its errors.
type IBinder interface {
	Bind(r *http.Request, dst any) error
}

// newDefaultBinder returns bingo with go-playground/validator, whose messages name fields by their tag, and
// with validators registered as custom tags.
func newDefaultBinder(validators map[string]validator.Func) (IBinder, error) {
	val := validator.New(validator.WithRequiredStructEnabled())
	val.RegisterTagNameFunc(func(field reflect.StructField) string {
		for _, tag := range []string{"json", "query", "uri", "header"} {
			name := strings.SplitN(field.Tag.Get(tag), ",", 2)[0]

			if name != "" && name != "-" {
				return name
			}
		}

		return field.Name
	})
	for _, name := range slices.Sorted(maps.Keys(validators)) {
		// validator returns an error for an empty name or a nil function, and panics on a name it reserves,
		// such as "dive" or one with a comma.
		if err := val.RegisterValidation(name, validators[name]); err != nil {
			return nil, fmt.Errorf("http: validator %q: %w", name, err)
		}
	}
	binder, err := bingo.NewBinder(
		bingo.WithDecoder(bingo.JSONDecoder{DisallowUnknownFields: false, UseNumber: true}),
		bingo.WithValidator(val),
	)
	if err != nil {
		return nil, fmt.Errorf("http: binder: %w", err)
	}

	return binder, nil
}
