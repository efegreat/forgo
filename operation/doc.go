// Package operation defines the transport-agnostic unit of work every server runs. A transport
// translates its input, such as an HTTP request, a cron run, a consumed message or a CLI command, into
// a *Context and runs a Handler chain on it, so a Middleware works on every transport.
//
// Errors are Error values. Their Message is public and is all a transport sends. Their cause is
// private and only logged. ErrorMapper turns any other error into an Error.
//
// The package also holds the configs and contracts the built-in middlewares and servers share: Lock
// and ILocker for distributed locks, Tolerance for retries and Transactor for transactions.
package operation
