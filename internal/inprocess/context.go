// Package inprocess holds what the servers share to run an operation in process, for forgotest, outside
// their transports: from an HTTP router, a cron job's run or a consumer's subscriber.
package inprocess

import "context"

// Context returns the context of an operation a test runs in process. It ends with ctx, and looks a value up
// in ctx first, then in server: the context the operation derives from when the server's transport runs it,
// such as one that carries the server's logger.
func Context(ctx, server context.Context) context.Context {
	return merged{Context: ctx, server: server}
}

// merged ends with its Context, and adds the values of server to it.
type merged struct {
	context.Context
	server context.Context
}

func (c merged) Value(key any) any {
	if v := c.Context.Value(key); v != nil {
		return v
	}
	return c.server.Value(key)
}
