package http

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/efegreat/forgo/operation"

	"github.com/efegreat/bingo"
	"github.com/go-playground/validator/v10"
)

var (
	errBodyTooLarge = operation.Error{
		Code:    "body_too_large",
		Status:  operation.StatusTooLarge,
		Message: "request body larger than %d bytes",
	}
	errBodyEOF = operation.Error{
		Code:    "eof",
		Status:  operation.StatusInvalidArgument,
		Message: "request body EOF",
	}
	errJSONSyntax = operation.Error{
		Code:    "json_syntax",
		Status:  operation.StatusInvalidArgument,
		Message: "invalid json syntax",
	}
	errJSONType = operation.Error{
		Code:    "json_type",
		Status:  operation.StatusInvalidArgument,
		Message: "type error, field: %s, required type: %s",
	}
	errInvalidBody = operation.Error{
		Code:    "invalid_body",
		Status:  operation.StatusInvalidArgument,
		Message: "invalid request body",
	}
)

func newDefaultErrorMapper() *operation.ErrorMapper {
	return operation.NewErrorMapper(
		MapBody,
		MapBinding,
		MapValidation,
	)
}

// MapBody maps the failures to read and decode a request body: a body over the server's MaxBodyBytes
// (body_too_large, 413), and, all answered 400, a truncated body (eof), malformed JSON (json_syntax), a JSON
// value of the wrong type (json_type) and anything else the default binder rejects in a body (invalid_body), such
// as two JSON documents or a field that is bound from a header.
//
// It maps only errors of the body: the bingo.BodyError of the default binder, and the *http.MaxBytesError of a
// read past the limit. A handler error that wraps io.EOF or a JSON error, such as from a call to another
// service, stays a server error.
func MapBody(err error) (operation.Error, bool) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return errBodyTooLarge.Format(tooLarge.Limit).Wrap(tooLarge), true
	}

	var be bingo.BodyError
	if !errors.As(err, &be) {
		return operation.Error{}, false
	}
	var syntax *json.SyntaxError
	var typ *json.UnmarshalTypeError
	switch cause := be.Cause; {
	case errors.Is(cause, io.EOF), errors.Is(cause, io.ErrUnexpectedEOF):
		return errBodyEOF.Wrap(cause), true
	case errors.As(cause, &syntax):
		return errJSONSyntax.Wrap(syntax), true
	case errors.As(cause, &typ):
		return errJSONType.Format(typ.Field, typ.Type).Wrap(typ), true
	default:
		return errInvalidBody.Wrap(cause), true
	}
}

// MapValidation maps the validator.ValidationErrors of the validate tags (validation, 400). The message names
// every failed field and rule, such as "field: name, rule: min=2".
func MapValidation(err error) (operation.Error, bool) {
	var ve validator.ValidationErrors
	if !errors.As(err, &ve) {
		return operation.Error{}, false
	}

	var messages []string

	for _, fe := range ve {
		rule := fe.Tag()
		if fe.Param() != "" {
			rule += "=" + fe.Param()
		}

		messages = append(messages,
			"field: "+fe.Field()+", rule: "+rule,
		)
	}

	return operation.Error{
		Code:    "validation",
		Status:  operation.StatusInvalidArgument,
		Message: strings.Join(messages, "; "),
	}.Wrap(ve), true
}

// MapBinding maps the bingo.FieldError of a query, path or header value that cannot be bound (binding, 400),
// such as "invalid param for query: page". The cause, which may quote the value, is only logged.
func MapBinding(err error) (operation.Error, bool) {
	var fe bingo.FieldError
	if !errors.As(err, &fe) {
		return operation.Error{}, false
	}

	return operation.Error{
		Code:    "binding",
		Status:  operation.StatusInvalidArgument,
		Message: "invalid param for " + fe.Source.String() + ": " + fe.Name,
	}.Wrap(fe.Unwrap()), true
}
