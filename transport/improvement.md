# transport: improvements

Findings from the pre-v1 review of `transport/`.

## Must have

None.

## Should have

None.

## Could have

### Type the argument of `ISerializer.Error`

Every caller passes a mapped error, and `operation.ErrorMapper.Map` returns `operation.Error`. `Error(err operation.Error) error` would tell implementers what they get, and it would remove the plain-text `500` branch of the default serializer (`server/http/serializer.go`). It breaks every custom serializer, for documentation value only.

### Keep zero-copy writes on observed connections

The wrapper could forward `io.ReaderFrom`, `io.WriterTo` and `syscall.Conn` when the accepted connection has them, and report the write errors of `ReadFrom`. This matters little today. Handlers write through `server/http`'s `responseRecorder`, which hides `ReadFrom` anyway (`server/http/logging.go:157`), and only `IConnIOObserver`s get a wrapper.

### Tell idle timeouts apart from slow clients

An idle keep-alive connection reaching `IdleTimeout` and a client too slow for `ReadHeaderTimeout` report the same `os.ErrDeadlineExceeded`. Telling them apart at the connection level would mean guessing net/http's request cycle from the order of reads and writes, which breaks on hijacked and HTTP/2 connections. The difference is documented instead. An observer that only wants resets can skip `os.ErrDeadlineExceeded`.

### `ISerializer` is HTTP-only in practice

Only `server/http` uses it, and its factory takes an `http.ResponseWriter`. It stays in `transport` for future transports such as gRPC. Moving it would only cause churn.

## Performance

Without an `IConnIOObserver`, which is the default, connections are not wrapped. `Listener.Accept` adds a nil check and a type assertion per connection. With one, each connection costs one allocation (40 B), an interface call per `Read`/`Write`, and an atomic store per deadline change. A non-zero deadline also costs one `time.Now`, and net/http sets a few per request. Failures add a compare-and-swap. Neither `Listener` (32 B) nor the wrapper (two interfaces and two `atomic.Int32`s) has padding to remove.
