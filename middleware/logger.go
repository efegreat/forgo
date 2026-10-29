package middleware

import (
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"

	"github.com/rs/zerolog"
)

// FieldAdder adds fields to the logger Logger derives for each operation, so that every line logged
// during the operation carries them. It runs once per operation, on the request path, so it should only
// read what the transport already holds, such as the operation's State.
type FieldAdder func(ctx *operation.Context, zCtx zerolog.Context) zerolog.Context

// Logger derives the operation's logger from the one in its context and stores it in the context, so
// every log line of the operation carries op_type, op_name and op_id, plus the fields that adder adds.
// A nil adder adds nothing.
//
// It logs ctx.ID, so ID must run before it.
func Logger(adder FieldAdder) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			base := logger.FromCtx(ctx)
			lCtx := base.With().
				Str(logger.OperationTypeLogKey, ctx.Type).
				Str(logger.OperationNameLogKey, ctx.Name).
				Str(logger.OperationIDLogKey, ctx.ID)

			if adder != nil {
				lCtx = adder(ctx, lCtx)
			}

			lg := lCtx.Logger()
			ctx.Context = logger.NewCtx(ctx.Context, lg)

			return next(ctx)
		}
	}
}
