package http

import (
	"net/http"

	"github.com/efegreat/forgo/operation"
)

var statusMap = map[operation.Status]int{
	operation.StatusInvalidArgument:    http.StatusBadRequest,
	operation.StatusUnauthenticated:    http.StatusUnauthorized,
	operation.StatusPermissionDenied:   http.StatusForbidden,
	operation.StatusNotFound:           http.StatusNotFound,
	operation.StatusConflict:           http.StatusConflict,
	operation.StatusAborted:            http.StatusConflict,
	operation.StatusPreconditionFailed: http.StatusPreconditionFailed,
	operation.StatusRateLimited:        http.StatusTooManyRequests,
	operation.StatusCancelled:          http.StatusRequestTimeout,
	operation.StatusTimeout:            http.StatusRequestTimeout,
	operation.StatusUnavailable:        http.StatusServiceUnavailable,
	operation.StatusInternal:           http.StatusInternalServerError,
	operation.StatusTooLarge:           http.StatusRequestEntityTooLarge,
	operation.StatusUnimplemented:      http.StatusNotImplemented,
}

// status converts a framework status into an HTTP status code.
// Unknown statuses default to 500.
func status(status operation.Status) int {
	if s, ok := statusMap[status]; ok {
		return s
	}

	return http.StatusInternalServerError
}
