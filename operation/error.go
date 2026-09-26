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
)

// ErrorSeverity defines the severity level of an error. It helps categorize errors based on their impact on the system or operation.
type ErrorSeverity int

const (
	// SeverityLight indicates a minor incident that needs attention but does not impact operations.
	SeverityLight ErrorSeverity = iota
	// SeverityModerate indicates a significant issue that may affect performance or user experience but is not critical.
	SeverityModerate
	// SeverityCritical indicates a critical issue that impacts the system's functionality. It needs to be handled as soon as possible.
	SeverityCritical
)

// Error represents an operation error with a code, status, message, severity and an optional underlying error.
type Error struct {
	// Code is the identifier for the error, which can be used for programmatic handling.
	Code string
	// Status is the transport-agnostic classification of the error.
	Status Status
	// Message provides a human-readable description of the error. This is the public error message.
	Message string
	// Severity is the severity level of the error
	Severity ErrorSeverity
	// err is the underlying error that caused this operation error, if any. This is the internal (private) error message.
	err error
}

func (e Error) Error() string {
	return e.Message
}

func (e Error) Wrap(err error) Error {
	e.err = err
	return e
}

func (e Error) Unwrap() error {
	return e.err
}

func (e Error) Format(args ...any) Error {
	e.Message = fmt.Sprintf(e.Message, args...)
	return e
}

// NewFallbackError returns an Error with cause err. This is the fallback when we don't know the source of err.
func NewFallbackError(err error) Error {
	return Error{
		Code:     "x",
		Status:   StatusUnknown,
		Message:  "an error occurred",
		Severity: SeverityCritical,
		err:      err,
	}
}

// ErrorMapperFunc is used to check if an err has a defined corresponding Error. If so, the Error and true is returned, if not, a zero Error and false.
type ErrorMapperFunc func(err error) (Error, bool)

// ErrorMapper is used to map errs to Error with ErrorMapperFunc definitions.
type ErrorMapper struct {
	mappers []ErrorMapperFunc
}

func NewErrorMapper(mappers ...ErrorMapperFunc) *ErrorMapper {
	return &ErrorMapper{
		mappers: append(mappers, mappers...),
	}
}

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
