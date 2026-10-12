package middleware

import (
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
)

// Logger derives a logger for each operation, with its type, name and ID plus the fields of adder, and
// puts it into the operation's context, so that every line logged during the operation carries them.
// A nil adder adds nothing.
func Logger(adder logger.FieldAdder) operation.Middleware {
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
