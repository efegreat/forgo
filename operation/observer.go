package operation

import "time"

// Observation is the outcome of an operation, as middleware.Observe reports it to an IObserver.
type Observation struct {
	// Elapsed is the time the operation took, inside Observe.
	Elapsed time.Duration
	// Err is the error the operation failed with, nil on success.
	Err error
	// Error is ErrorOf(Err): the code, status and severity the operation failed with. It is zero on success.
	Error Error
}

// IObserver is told about the operations of every server, for metrics. Servers resolve it from the
// bootstrap, as an optional dependency, and put middleware.Observe in their base chain when there is one.
//
// The labels it records should be those of Observation.Error, the operation's Type and Name, which are the
// same on every transport, and the HTTP status or gRPC code the client got. They must stay bounded: ID,
// message IDs and raw paths are not.
//
// It is called from every operation's goroutine, so it must be safe for concurrent use and must not block.
type IObserver interface {
	// Start is called before the operation runs, such as for in-flight gauges. It may replace ctx.Context,
	// as a middleware may.
	Start(ctx *Context)
	// End is called once the operation ended. In the base chains, ctx.State is final then: the HTTP response
	// is written and the gRPC status is chosen.
	End(ctx *Context, o Observation)
}
