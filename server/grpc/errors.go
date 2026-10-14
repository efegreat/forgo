package grpc

import (
	"context"
	"errors"
	"strings"
	"unicode"

	"github.com/efegreat/forgo/operation"

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
		MapValidation,
		MapStatus,
		MapContext,
	)
}

// ErrorResponder answers the error of the rest of the chain, mapped by em, unless the call is already
// answered. It sits inside the Recorder of the base chain, so the Recorder logs the code and severity the
// client gets. domain is the Domain of the errdetails.ErrorInfo sent with every error.
func ErrorResponder(em *operation.ErrorMapper, domain string) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			return respondError(ctx, em, domain, next(ctx))
		}
	}
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

// validationError is a message the validator rejected. Its text names the message, and MapValidation
// sends the validator's text, so the log reads "<validator's text>: invalid <message>".
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

// validateMessage validates msg with validate, and returns its failure as a validationError.
func validateMessage(validate func(proto.Message) error, msg proto.Message) error {
	if err := validate(msg); err != nil {
		return validationError{message: string(msg.ProtoReflect().Descriptor().FullName()), err: err}
	}
	return nil
}

// defaultValidator calls the Validate method of messages that have one, such as those protoc-gen-validate
// generates.
func defaultValidator(msg proto.Message) error {
	if v, ok := msg.(interface{ Validate() error }); ok {
		return v.Validate()
	}
	return nil
}

// MapValidation maps the failures of Config.Validator (validation, InvalidArgument). The message is the
// validator's error text, which the client gets.
func MapValidation(err error) (operation.Error, bool) {
	var ve validationError
	if !errors.As(err, &ve) {
		return operation.Error{}, false
	}
	return operation.Error{
		Code:    "validation",
		Status:  operation.StatusInvalidArgument,
		Message: ve.err.Error(),
	}.Wrap(ve), true
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
