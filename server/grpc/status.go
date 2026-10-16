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
	operation.StatusTooLarge:           codes.ResourceExhausted,
	operation.StatusUnimplemented:      codes.Unimplemented,
}

// operationStatusMap is the reverse of grpcStatusMap, for the statuses that grpc-go or a handler send
// themselves. Codes without a status of their own get the closest one.
var operationStatusMap = map[codes.Code]operation.Status{
	codes.Canceled:           operation.StatusCancelled,
	codes.InvalidArgument:    operation.StatusInvalidArgument,
	codes.DeadlineExceeded:   operation.StatusTimeout,
	codes.NotFound:           operation.StatusNotFound,
	codes.AlreadyExists:      operation.StatusConflict,
	codes.PermissionDenied:   operation.StatusPermissionDenied,
	codes.ResourceExhausted:  operation.StatusRateLimited,
	codes.FailedPrecondition: operation.StatusPreconditionFailed,
	codes.Aborted:            operation.StatusAborted,
	codes.OutOfRange:         operation.StatusInvalidArgument,
	codes.Unimplemented:      operation.StatusUnimplemented,
	codes.Internal:           operation.StatusInternal,
	codes.Unavailable:        operation.StatusUnavailable,
	codes.DataLoss:           operation.StatusInternal,
	codes.Unauthenticated:    operation.StatusUnauthenticated,
}

// gRPCStatus converts a framework status into a gRPC status code.
// Unknown statuses default to codes.Internal.
func gRPCStatus(status operation.Status) codes.Code {
	if s, ok := grpcStatusMap[status]; ok {
		return s
	}

	return codes.Internal
}

// operationStatus converts a gRPC status code into a framework status. codes.Unknown and codes outside
// the gRPC specification are StatusUnknown.
func operationStatus(code codes.Code) operation.Status {
	return operationStatusMap[code]
}
