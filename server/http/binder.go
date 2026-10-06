package http

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/efegreat/bingo"
	"github.com/go-playground/validator/v10"
)

// IBinder is an interface that defines a method for binding an HTTP request to a Go struct. It is used to extract data from the request and populate the fields of the struct.
type IBinder interface {
	Bind(*http.Request, any) error
}

func newDefaultBinder(validators map[string]validator.Func) IBinder {
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
	for name, fn := range validators {
		val.RegisterValidation(name, fn)
	}
	binder, _ := bingo.NewBinder(
		bingo.WithDecoder(bingo.JSONDecoder{DisallowUnknownFields: false, UseNumber: true}),
		bingo.WithValidator(val),
	)

	return binder
}
