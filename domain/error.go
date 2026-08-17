package domain

import (
	"fmt"
)

// Error represents a domain error with a code, status, message, and an optional underlying error.
type Error struct {
	// Code is the identifier for the error, which can be used for programmatic handling.
	Code string
	// Status is the transport-agnostic classification of the error.
	Status Status
	// Message provides a human-readable description of the error. This is the public error message.
	Message string
	// err is the underlying error that caused this domain error, if any. This is the internal (private) error message.
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
