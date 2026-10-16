package grpc

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"unicode"

	"github.com/efegreat/forgo/operation"

	"github.com/go-playground/validator/v10"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// codeNames are the operation.Error codes of the gRPC status codes, in snake case.
var codeNames = func() map[codes.Code]string {
	names := make(map[codes.Code]string, codes.Unauthenticated+1)
	for c := codes.OK; c <= codes.Unauthenticated; c++ {
		var b strings.Builder
		for i, r := range c.String() {
			if unicode.IsUpper(r) {
				if i > 0 {
					b.WriteByte('_')
				}
				r = unicode.ToLower(r)
			}
			b.WriteRune(r)
		}
		names[c] = b.String()
	}
	return names
}()

func newDefaultErrorMapper() *operation.ErrorMapper {
	return operation.NewErrorMapper(
		MapBinding,
		MapValidation,
		MapStatus,
		MapContext,
	)
}

// Responder answers the call once the rest of the chain returns. When it returns nil, Responder sends
// State.Response, the one response of a unary or client-streaming method, so that a middleware may replace
// it, or answer without calling next. Otherwise it answers the error, mapped by em, unless the call is
// already answered. It sits inside the Recorder of the base chain, so the Recorder logs a failed send, and
// the code and severity the client gets. domain is the Domain of the errdetails.ErrorInfo sent with every
// error.
func Responder(em *operation.ErrorMapper, domain string) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			return respond(ctx, em, domain, next(ctx))
		}
	}
}

// respond sends the response of a call whose chain returned nil, or answers err. It does nothing for a call
// already answered, so that the server can call it again after a chain without base middlewares.
func respond(ctx *operation.Context, em *operation.ErrorMapper, domain string, err error) error {
	if err == nil {
		err = sendResponse(ctx.State.(*State))
	}
	return respondError(ctx, em, domain, err)
}

// sendResponse sends the one response of a unary or client-streaming method, once.
func sendResponse(st *State) error {
	if !st.kind.respondsOnce() || st.sent || st.answered() {
		return nil
	}
	if st.Response == nil {
		return errNilResponse
	}
	st.sent = true
	if err := st.stream.SendMsg(st.Response); err != nil {
		return st.fail(send, err)
	}
	return nil
}

// respondError chooses the status that answers err, mapped by em, unless err is nil or the call is already
// answered. It returns the error the operation fails with, which Recorder logs: err itself when it holds an
// operation.Error, so the log keeps what is wrapped around it, such as a panic's stack, and otherwise the
// mapped error, so the log has the code the client got.
func respondError(ctx *operation.Context, em *operation.ErrorMapper, domain string, err error) error {
	if err == nil {
		return nil
	}
	st := ctx.State.(*State)
	if st.answered() {
		return err
	}

	failed := err
	var oe operation.Error
	if !errors.As(err, &oe) {
		oe = em.Map(err)
		// A mapper that leaves err out of its result would leave it out of the log too.
		if oe.Unwrap() == nil {
			oe = oe.Wrap(err)
		}
		failed = oe
	}
	st.answer = toStatus(oe, domain)
	return failed
}

// toStatus returns the status of oe: its Message, the code of its Status and an errdetails.ErrorInfo with
// its Code as Reason. The status of an error that MapStatus mapped is sent as it is, details included.
// Error() is never sent, since it holds the private cause.
func toStatus(oe operation.Error, domain string) *status.Status {
	if c, ok := oe.Unwrap().(statusCause); ok {
		return c.status
	}
	s := status.New(gRPCStatus(oe.Status), oe.Message)
	if oe.Code == "" {
		return s
	}
	if withInfo, err := s.WithDetails(&errdetails.ErrorInfo{Reason: oe.Code, Domain: domain}); err == nil {
		return withInfo
	}
	return s
}

// validationError is a request the validator rejected. Its text names the request type, and MapValidation
// sends the validator's text, so the log reads "<validator's text>: invalid <type>".
type validationError struct {
	message string
	err     error
}

func (e validationError) Error() string {
	return "invalid " + e.message
}

func (e validationError) Unwrap() error {
	return e.err
}

// newDefaultValidator returns the default Config.Validator: go-playground/validator on the validate tags of
// a struct, whose messages name fields by their tags as over HTTP, with validators registered as custom tags,
// then the Validate() error method of a request that has one, such as a message protoc-gen-validate generates.
// It also returns checkTags, which New calls on every request type.
func newDefaultValidator(validators map[string]validator.Func) (validate func(any) error, checkTags func(reflect.Type), err error) {
	val := validator.New(validator.WithRequiredStructEnabled())
	val.RegisterTagNameFunc(func(field reflect.StructField) string {
		for _, tag := range []string{"grpc", "json", "query", "uri", "header"} {
			if name, _, _ := strings.Cut(field.Tag.Get(tag), ","); name != "" && name != "-" {
				return name
			}
		}
		return field.Name
	})
	for _, name := range slices.Sorted(maps.Keys(validators)) {
		// validator returns an error for an empty name or a nil function, and panics on a name it reserves,
		// such as "dive" or one with a comma.
		if err := val.RegisterValidation(name, validators[name]); err != nil {
			return nil, nil, fmt.Errorf("grpc: validator %q: %w", name, err)
		}
	}
	validate = func(req any) error {
		if _, ok := req.(proto.Message); !ok {
			if err := val.Struct(req); err != nil {
				return err
			}
		}
		if v, ok := req.(interface{ Validate() error }); ok {
			return v.Validate()
		}
		return nil
	}
	return validate, func(t reflect.Type) { checkValidateTags(val, t, map[reflect.Type]bool{}) }, nil
}

// checkValidateTags validates an empty value of t, and of every struct t holds, so that a tag
// go-playground/validator does not know, such as a custom one an HTTP server registers but this server does
// not, panics in New, at startup, rather than on the first call. An empty value only validates the fields it
// holds, so nested structs are checked one by one.
func checkValidateTags(val *validator.Validate, t reflect.Type, seen map[reflect.Type]bool) {
	for {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			t = t.Elem()
			continue
		case reflect.Map:
			checkValidateTags(val, t.Key(), seen)
			t = t.Elem()
			continue
		}
		break
	}
	if t.Kind() != reflect.Struct || seen[t] || reflect.PointerTo(t).Implements(protoMessageType) {
		return
	}
	seen[t] = true
	_ = val.Struct(reflect.New(t).Interface())
	for i := range t.NumField() {
		if f := t.Field(i); f.IsExported() || f.Anonymous {
			checkValidateTags(val, f.Type, seen)
		}
	}
}

// MapValidation maps the failures of Config.Validator (validation, InvalidArgument). The message of
// validator.ValidationErrors names every failed field and rule, such as "field: name, rule: min=2", as over
// HTTP. That of any other error is its text, which the client gets.
func MapValidation(err error) (operation.Error, bool) {
	var ve validationError
	if !errors.As(err, &ve) {
		return operation.Error{}, false
	}
	message := ve.err.Error()
	var fields validator.ValidationErrors
	if errors.As(ve.err, &fields) {
		messages := make([]string, len(fields))
		for i, fe := range fields {
			rule := fe.Tag()
			if fe.Param() != "" {
				rule += "=" + fe.Param()
			}
			messages[i] = "field: " + fe.Field() + ", rule: " + rule
		}
		message = strings.Join(messages, "; ")
	}
	return operation.Error{
		Code:    "validation",
		Status:  operation.StatusInvalidArgument,
		Message: message,
	}.Wrap(ve), true
}

// MapBinding maps a request value that the handler's type cannot hold (binding, InvalidArgument), such as a
// number out of its range or a string its UnmarshalText rejects: "invalid field: address.zip", or "invalid
// metadata: x-tenant-id" for a field tagged header. The cause, which may quote the value, is only logged.
func MapBinding(err error) (operation.Error, bool) {
	var fe *fieldError
	if !errors.As(err, &fe) {
		return operation.Error{}, false
	}
	return operation.Error{
		Code:    "binding",
		Status:  operation.StatusInvalidArgument,
		Message: "invalid " + fe.source + ": " + fe.Path(),
	}.Wrap(fe.cause), true
}

// statusCause is the cause MapStatus gives the operation.Error of a status, so that toStatus sends that
// status.
type statusCause struct {
	status *status.Status
	err    error
}

// Error returns the text of the mapped error, or only the code when the mapped error is the status error
// itself, whose message the operation.Error already holds.
func (c statusCause) Error() string {
	if _, ok := c.err.(interface{ GRPCStatus() *status.Status }); ok {
		return "code = " + c.status.Code().String()
	}
	return c.err.Error()
}

func (c statusCause) Unwrap() error {
	return c.err
}

// MapStatus maps an error that carries a gRPC status, such as one a handler returned with status.Error or
// one of a gRPC service it called, to an operation.Error with the status's message. The client gets that
// status as it is, details included. The Code is the Reason of the status's errdetails.ErrorInfo, or the name
// of its code in snake case, such as "not_found". Unknown, Internal and DataLoss are critical, Unavailable is
// moderate and the other codes are light.
func MapStatus(err error) (operation.Error, bool) {
	var se interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &se) {
		return operation.Error{}, false
	}
	s := se.GRPCStatus()
	if s.Code() == codes.OK {
		return operation.Error{}, false
	}

	code := codeNames[s.Code()]
	for _, d := range s.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() != "" {
			code = info.GetReason()
			break
		}
	}
	severity := operation.SeverityLight
	switch s.Code() {
	case codes.Unknown, codes.Internal, codes.DataLoss:
		severity = operation.SeverityCritical
	case codes.Unavailable:
		severity = operation.SeverityModerate
	}
	return operation.Error{
		Code:     code,
		Status:   operationStatus(s.Code()),
		Message:  s.Message(),
		Severity: severity,
	}.Wrap(statusCause{status: s, err: err}), true
}

// MapContext maps context.Canceled (canceled, Canceled) and context.DeadlineExceeded (deadline_exceeded,
// DeadlineExceeded), as when a handler returns ctx.Err() after the client canceled or its deadline passed.
// Both are light.
func MapContext(err error) (operation.Error, bool) {
	switch {
	case errors.Is(err, context.Canceled):
		return errCanceled.Wrap(err), true
	case errors.Is(err, context.DeadlineExceeded):
		return errDeadlineExceeded.Wrap(err), true
	default:
		return operation.Error{}, false
	}
}
