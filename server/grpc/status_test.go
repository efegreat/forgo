package grpc

import (
	"testing"

	"github.com/efegreat/forgo/operation"

	"google.golang.org/grpc/codes"
)

func TestGRPCStatus(t *testing.T) {
	for st, want := range map[operation.Status]codes.Code{
		operation.StatusUnknown:            codes.Internal,
		operation.StatusInvalidArgument:    codes.InvalidArgument,
		operation.StatusUnauthenticated:    codes.Unauthenticated,
		operation.StatusPermissionDenied:   codes.PermissionDenied,
		operation.StatusNotFound:           codes.NotFound,
		operation.StatusConflict:           codes.AlreadyExists,
		operation.StatusAborted:            codes.Aborted,
		operation.StatusPreconditionFailed: codes.FailedPrecondition,
		operation.StatusTooLarge:           codes.ResourceExhausted,
		operation.StatusRateLimited:        codes.ResourceExhausted,
		operation.StatusCancelled:          codes.Canceled,
		operation.StatusTimeout:            codes.DeadlineExceeded,
		operation.StatusUnavailable:        codes.Unavailable,
		operation.StatusInternal:           codes.Internal,
		operation.StatusUnimplemented:      codes.Unimplemented,
		operation.Status(200):              codes.Internal,
	} {
		if got := gRPCStatus(st); got != want {
			t.Errorf("gRPCStatus(%d) = %s, want %s", st, got, want)
		}
	}
}
