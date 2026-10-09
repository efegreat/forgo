package http

import (
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/efegreat/forgo/operation"

	"github.com/efegreat/bingo"
	"github.com/go-playground/validator/v10"
)

func newDefaultErrorMapper() *operation.ErrorMapper {
	return operation.NewErrorMapper(
		MapEOF,
		MapJSONSyntax,
		MapJSONType,
		MapBinding,
		MapValidation,
	)
}

// MapEOF maps an empty or truncated request body.
func MapEOF(err error) (operation.Error, bool) {
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return operation.Error{}, false
	}

	return operation.Error{
		Code:    "eof",
		Status:  operation.StatusInvalidArgument,
		Message: "request body EOF",
	}.Wrap(err), true
}

func MapJSONSyntax(err error) (operation.Error, bool) {
	var je *json.SyntaxError
	if !errors.As(err, &je) {
		return operation.Error{}, false
	}

	return operation.Error{
		Code:    "json_syntax",
		Status:  operation.StatusInvalidArgument,
		Message: "invalid json syntax",
	}.Wrap(je), true
}

func MapJSONType(err error) (operation.Error, bool) {
	var ute *json.UnmarshalTypeError
	if !errors.As(err, &ute) {
		return operation.Error{}, false
	}

	return operation.Error{
		Code:    "json_type",
		Status:  operation.StatusInvalidArgument,
		Message: "type error, field: " + ute.Field + ", required type: " + ute.Type.String(),
	}.Wrap(ute), true
}

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
