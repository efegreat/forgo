package grpc

import (
	"forgo/domain"

	"google.golang.org/grpc/codes"
)

var grpcStatusMap = map[domain.Status]codes.Code{
	domain.StatusInvalidArgument:    codes.InvalidArgument,
	domain.StatusUnauthenticated:    codes.Unauthenticated,
	domain.StatusPermissionDenied:   codes.PermissionDenied,
	domain.StatusNotFound:           codes.NotFound,
	domain.StatusConflict:           codes.AlreadyExists,
	domain.StatusAborted:            codes.Aborted,
	domain.StatusPreconditionFailed: codes.FailedPrecondition,
	domain.StatusRateLimited:        codes.ResourceExhausted,
	domain.StatusCancelled:          codes.Canceled,
	domain.StatusTimeout:            codes.DeadlineExceeded,
	domain.StatusUnavailable:        codes.Unavailable,
	domain.StatusInternal:           codes.Internal,
}

// gRPCStatus converts a framework status into a gRPC status code.
// Unknown statuses default to codes.Internal.
func gRPCStatus(status domain.Status) codes.Code {
	if s, ok := grpcStatusMap[status]; ok {
		return s
	}

	return codes.Internal
}
