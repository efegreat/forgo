package grpc

import (
	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc/codes"
)

var grpcStatusMap = map[operation.Status]codes.Code{
	operation.StatusInvalidArgument:    codes.InvalidArgument,
	operation.StatusUnauthenticated:    codes.Unauthenticated,
	operation.StatusPermissionDenied:   codes.PermissionDenied,
	operation.StatusNotFound:           codes.NotFound,
	operation.StatusConflict:           codes.AlreadyExists,
	operation.StatusAborted:            codes.Aborted,
	operation.StatusPreconditionFailed: codes.FailedPrecondition,
	operation.StatusRateLimited:        codes.ResourceExhausted,
	operation.StatusCancelled:          codes.Canceled,
	operation.StatusTimeout:            codes.DeadlineExceeded,
	operation.StatusUnavailable:        codes.Unavailable,
	operation.StatusInternal:           codes.Internal,
}

// gRPCStatus converts a framework status into a gRPC status code.
// Unknown statuses default to codes.Internal.
func gRPCStatus(status operation.Status) codes.Code {
	if s, ok := grpcStatusMap[status]; ok {
		return s
	}

	return codes.Internal
}
