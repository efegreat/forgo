package middleware

import (
	"github.com/efegreat/forgo/operation"

	"github.com/google/uuid"
)

// maxIDLen bounds the incoming IDs that ID keeps. It fits every common format, such as UUIDs, ULIDs
// and W3C trace IDs.
const maxIDLen = 128

// ID gives every operation an ID that correlates its logs and its response.
//
// It keeps an ID that the transport took from its input, such as the HTTP X-Request-ID header, when it
// is at most 128 bytes of visible ASCII. Otherwise, and when the ID is empty, it sets a new UUIDv7. An
// incoming ID comes from the client: it appears on every log line of the operation and is echoed in the
// response, so an oversized or malformed one is replaced, never kept.
func ID() operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if !validID(ctx.ID) {
				ctx.ID = uuid.Must(uuid.NewV7()).String()
			}
			return next(ctx)
		}
	}
}

// validID reports whether id is non-empty, at most maxIDLen bytes, and only visible ASCII: no spaces,
// control characters or bytes above 0x7e.
func validID(id string) bool {
	if id == "" || len(id) > maxIDLen {
		return false
	}
	for i := range len(id) {
		if id[i] < '!' || id[i] > '~' {
			return false
		}
	}
	return true
}
