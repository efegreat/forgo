package middleware

import (
	"forgo/logger"
	"forgo/operation"
)

// Logger is a middleware that builds a logger for each operation and adds it to the context. It takes a function that modifies the zerolog.Context and returns a new zerolog.Context. This allows for customizing the logger for each operation.
func Logger(adder logger.FieldAdder) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			base := logger.FromCtx(ctx.Context)
			lg := adder(base.With().
				Str(logger.OperationTypeLogKey, ctx.Type).
				Str(logger.OperationNameLogKey, ctx.Name).
				Str(logger.OperationIDLogKey, ctx.ID),
			).Logger()
			ctx.Context = logger.NewCtx(ctx.Context, lg)

			return next(ctx)
		}
	}
}
