package operation

import (
	"errors"
	"fmt"
)

// Status represents a transport-agnostic classification of an error.
//
// It intentionally does not mirror HTTP or gRPC status codes.
// Instead, transports map these semantic statuses to their own protocol codes.
type Status uint8

const (
	// StatusUnknown is used when no better status is known.
	// Transport fallbacks:
	//   HTTP -> 500 Internal Server Error
	//   gRPC -> Internal
	StatusUnknown Status = iota

	// StatusInvalidArgument indicates the client supplied invalid input.
	// HTTP -> 400
	// gRPC -> InvalidArgument
	StatusInvalidArgument

	// StatusUnauthenticated indicates authentication is required or invalid.
	// HTTP -> 401
	// gRPC -> Unauthenticated
	StatusUnauthenticated

	// StatusPermissionDenied indicates the caller is authenticated but
	// not allowed to perform the operation.
	// HTTP -> 403
	// gRPC -> PermissionDenied
	StatusPermissionDenied

	// StatusNotFound indicates the requested resource does not exist.
	// HTTP -> 404
	// gRPC -> NotFound
	StatusNotFound

	// StatusConflict indicates the requested operation conflicts with
	// existing business state.
	//
	// Examples:
	//   - User already exists
	//   - Email already taken
	//   - Duplicate order number
	//
	// HTTP -> 409 Conflict
	// gRPC -> AlreadyExists
	StatusConflict

	// StatusAborted indicates the operation was aborted because the
	// resource changed concurrently or a transaction could not complete.
	//
	// Examples:
	//   - Optimistic lock failure
	//   - Serialization failure
	//   - Compare-and-swap failed
	//   - Transaction aborted
	//
	// Clients may retry after refreshing state.
	//
	// HTTP -> 409 Conflict
	// gRPC -> Aborted
	StatusAborted

	// StatusPreconditionFailed indicates the operation cannot be performed
	// because business preconditions are not met.
	//
	// Examples:
	//   - Order already paid
	//   - Resource is locked
	//   - Account not verified
	//
	// HTTP -> 412
	// gRPC -> FailedPrecondition
	StatusPreconditionFailed

	// StatusRateLimited indicates the client exceeded a limit.
	// HTTP -> 429
	// gRPC -> ResourceExhausted
	StatusRateLimited

	// StatusCancelled indicates the operation was cancelled.
	// HTTP -> 408
	// gRPC -> Canceled
	StatusCancelled

	// StatusTimeout indicates the operation timed out.
	// HTTP -> 408
	// gRPC -> DeadlineExceeded
	StatusTimeout

	// StatusUnavailable indicates a dependency or service is temporarily
	// unavailable.
	//
	// Examples:
	//   - Database unavailable
	//   - Redis unavailable
	//   - Third-party API unavailable
	//
	// HTTP -> 503
	// gRPC -> Unavailable
	StatusUnavailable

	// StatusInternal indicates an unexpected server error.
	// HTTP -> 500
	// gRPC -> Internal
	StatusInternal

	// StatusTooLarge indicates the input is larger than allowed, such as a
	// request body over the server's limit.
	// HTTP -> 413
	// gRPC -> ResourceExhausted
	StatusTooLarge

	// StatusUnimplemented indicates the operation is not implemented or
	// not supported, such as a call to a method the server does not serve.
	// HTTP -> 501
	// gRPC -> Unimplemented
	StatusUnimplemented
)

// statusNames are the names of the statuses, indexed by status.
var statusNames = [...]string{
	StatusUnknown:            "unknown",
	StatusInvalidArgument:    "invalid_argument",
	StatusUnauthenticated:    "unauthenticated",
	StatusPermissionDenied:   "permission_denied",
	StatusNotFound:           "not_found",
	StatusConflict:           "conflict",
	StatusAborted:            "aborted",
	StatusPreconditionFailed: "precondition_failed",
	StatusRateLimited:        "rate_limited",
	StatusCancelled:          "cancelled",
	StatusTimeout:            "timeout",
	StatusUnavailable:        "unavailable",
	StatusInternal:           "internal",
	StatusTooLarge:           "too_large",
	StatusUnimplemented:      "unimplemented",
}

// String returns the name of s in snake case, such as "not_found", so that every metrics backend labels
// statuses alike. A value outside the declared statuses is "unknown", like StatusUnknown.
func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return statusNames[StatusUnknown]
}

// ErrorSeverity classifies the impact of an error. Recorder logs a SeverityLight error as a warning
// and any other as an error, with the severity as a number for alert routing. The zero value is
// SeverityLight, so an Error declared without a severity is logged as a warning.
type ErrorSeverity uint8

const (
	// SeverityLight indicates a minor incident that needs attention but does not impact operations.
	SeverityLight ErrorSeverity = iota
	// SeverityModerate indicates a significant issue that may affect performance or user experience but is not critical.
	SeverityModerate
	// SeverityCritical indicates a critical issue that impacts the system's functionality. It needs to be handled as soon as possible.
	SeverityCritical
)

// Error is the error type every transport understands. Code, Status and Message are public: a
// transport sends Code and Message, and maps Status to its own protocol. The cause set with Wrap is
// private. Transports never send it, but Error() includes it, so logs and the errors that wrap this
// one keep it.
//
// Declare an Error once, as a package-level variable, and derive from it with Messagef and Wrap, which
// return copies. errors.Is matches the copies by Code.
type Error struct {
	// Code is the stable, machine-readable identifier of the error. Clients and errors.Is match on it.
	Code string
	// Status is the transport-agnostic classification of the error.
	Status Status
	// Message is the public, human-readable description of the error, safe to send to clients.
	Message string
	// Severity is the impact of the error, which picks its log level.
	Severity ErrorSeverity
	// err is the private cause of the error, if any.
	err error
}

// Error returns Message, then ": " and the cause when there is one, so that the text of an error
// that wraps this one keeps the whole chain. It is meant for logs. Send Message to clients instead, as
// the transports do, since the cause can hold internal details.
func (e Error) Error() string {
	switch {
	case e.err == nil:
		return e.Message
	case e.Message == "":
		return e.err.Error()
	default:
		return e.Message + ": " + e.err.Error()
	}
}

// Wrap returns a copy of e with err as its cause. A nil err removes the cause.
func (e Error) Wrap(err error) Error {
	e.err = err
	return e
}

// Unwrap returns the cause of e, or nil.
func (e Error) Unwrap() error {
	return e.err
}

// Is reports whether target is an Error with e's Code, so that errors.Is matches an Error after
// Messagef or Wrap changed its copy. An Error without a Code matches only an identical Error.
func (e Error) Is(target error) bool {
	t, ok := target.(Error)
	return ok && t.Code != "" && t.Code == e.Code
}

// As stores e in target when target is an *Error. errors.As matches an Error value without it, so it
// only matters for an *Error in the chain, which then counts as an Error everywhere instead of
// becoming the fallback error.
func (e Error) As(target any) bool {
	t, ok := target.(*Error)
	if ok {
		*t = e
	}
	return ok
}

// Messagef returns a copy of e whose Message is fmt.Sprintf(e.Message, args...), so that a declared
// Message can hold verbs for the details of each occurrence.
func (e Error) Messagef(args ...any) Error {
	e.Message = fmt.Sprintf(e.Message, args...)
	return e
}

// NewFallbackError returns the Error of an error nothing else describes: code "internal", StatusUnknown,
// message "an error occurred", SeverityCritical, and err as its cause.
func NewFallbackError(err error) Error {
	return Error{
		Code:     "internal",
		Status:   StatusUnknown,
		Message:  "an error occurred",
		Severity: SeverityCritical,
		err:      err,
	}
}

// ErrorOf returns the first Error in err's chain, or NewFallbackError(err) when there is none: the code,
// status and severity that err is logged, answered and observed with. Unlike errors.As with a pointer to
// an Error, it does not allocate, unless an error of the chain before the Error has an As method.
func ErrorOf(err error) Error {
	if oe, ok := errors.AsType[Error](err); ok {
		return oe
	}
	return NewFallbackError(err)
}

// ErrorMapperFunc returns the Error that describes err and true, or false when it does not know err.
type ErrorMapperFunc func(err error) (Error, bool)

// ErrorMapper turns any error into an Error, for transports that must answer with one. It is
// immutable and safe for concurrent use.
type ErrorMapper struct {
	mappers []ErrorMapperFunc
}

// NewErrorMapper returns an ErrorMapper that tries mappers in order. It copies the slice.
func NewErrorMapper(mappers ...ErrorMapperFunc) *ErrorMapper {
	return &ErrorMapper{
		mappers: append([]ErrorMapperFunc(nil), mappers...),
	}
}

// Map returns the first Error in err's chain unchanged, without running any mapper. Otherwise it
// returns the result of the first mapper that knows err, and NewFallbackError(err) when none does.
func (m *ErrorMapper) Map(err error) Error {
	var oe Error
	if errors.As(err, &oe) {
		return oe
	}

	for _, fn := range m.mappers {
		if oe, ok := fn(err); ok {
			return oe
		}
	}

	return NewFallbackError(err)
}
