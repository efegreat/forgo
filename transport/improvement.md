# transport: open improvements

These are the findings from the pre-v1 review of `transport/` that have not been applied yet. The applied ones are in the package code, `server/http`, REFERENCE.md and AGENTS.md:

- **`ISerializer.Error` contract.** The interface doc said that `Error` returns only write failures. The HTTP transport used its result as the result of the operation. A serializer that followed the doc (such as the one in `commands/openapi/generator_test.go`) had every failed request logged as `operation completed`, panics included. The doc now states the contract: return `err`, or `err` wrapped with the write failure. The HTTP server takes a `nil` result as the error it passed (`serializeError`).
- **Read and write hooks reported net/http's own work.** One keep-alive connection with 6 requests reported 6 `i/o timeout`s, one per aborted background read, plus an `EOF`. One header timeout reported 3 errors, because textproto retries the read. Shutdown reported `net.ErrClosed` to `OnAcceptError`. The hooks now skip `io.EOF`, `net.ErrClosed` and I/O canceled by a deadline already past when it was set. Each direction reports its first failure only, until its deadline changes. A closed listener reports nothing.
- **The connection wrapper hid `CloseWrite`**, so net/http could not send a FIN before closing a connection with an unread body (golang.org/issue/3595). It also hid the zero-copy paths and `ConnState`/`ConnContext` types on every connection. Now it forwards `CloseWrite`, has `NetConn()`, and wraps connections only for the new `IConnIOObserver`. `IConnObserver` keeps `OnAcceptError` alone. A server without an I/O observer hands net/http the accepted connection untouched.
- **`LoggingObserver` is removed.** It was the default, and it logged every accept error a second time, after net/http's `ErrorLog` (retried errors) or `App`'s `serve error` (the one `Serve` returns). Its other hooks were no-ops. `Observer` now defaults to nil, and `transport` depends on the standard library only.
- **Smaller fixes.** A nil `Observer` no longer panics in `Accept`. `Conn`, which could not be built outside the package, is unexported. The package has a doc comment, every exported identifier and hook states its contract, and tests cover the package fully, including against a real `http.Server`.

## Must have

None open.

## Should have

None open.

## Could have

### Type the argument of `ISerializer.Error`

Every caller passes a mapped error, and `operation.ErrorMapper.Map` returns `operation.Error`. `Error(err operation.Error) error` would tell implementers what they get, and it would remove the plain-text `500` branch of the default serializer (`server/http/serializer.go`). It breaks every custom serializer, for documentation value only.

### Keep zero-copy writes on observed connections

The wrapper could forward `io.ReaderFrom`, `io.WriterTo` and `syscall.Conn` when the accepted connection has them, and report the write errors of `ReadFrom`. This matters little today. Handlers write through `server/http`'s `responseRecorder`, which hides `ReadFrom` anyway (`server/http/logging.go:157`), and only `IConnIOObserver`s get a wrapper.

### Tell idle timeouts apart from slow clients

An idle keep-alive connection reaching `IdleTimeout` and a client too slow for `ReadHeaderTimeout` report the same `os.ErrDeadlineExceeded`. Telling them apart at the connection level would mean guessing net/http's request cycle from the order of reads and writes, which breaks on hijacked and HTTP/2 connections. The difference is documented instead. An observer that only wants resets can skip `os.ErrDeadlineExceeded`.

### `ISerializer` is HTTP-only in practice

Only `server/http` uses it, and its factory takes an `http.ResponseWriter`. It stays in `transport` for future transports such as gRPC. Moving it would only cause churn.

### Outside this package

`App` logs `serve error` without `logger.ErrorSeverityLogKey` (`app.go:98`), although AGENTS.md requires every error log to carry it. Fix it with the review of the root package.

## Performance

Without an `IConnIOObserver`, which is the default, connections are not wrapped. `Listener.Accept` adds a nil check and a type assertion per connection. With one, each connection costs one allocation (40 B), an interface call per `Read`/`Write`, and an atomic store per deadline change. A non-zero deadline also costs one `time.Now`, and net/http sets a few per request. Failures add a compare-and-swap. Neither `Listener` (32 B) nor the wrapper (two interfaces and two `atomic.Int32`s) has padding to remove.
