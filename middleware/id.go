package middleware

import (
	"forgo/operation"

	"github.com/google/uuid"
)

// ID is a middleware that generates a unique ID for each request if it doesn't already have one. It uses the UUID version 7 to create the ID and assigns it to the OperationCtx's ID field. This can be useful for tracking requests and correlating logs.
func ID() operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			if ctx.ID == "" {
				ctx.ID = uuid.Must(uuid.NewV7()).String()
			}
			return next(ctx)
		}
	}
}
