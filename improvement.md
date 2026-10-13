# forgo (root package): improvements

Findings from the pre-v1 review of the root package: `app.go`, `bootstrap.go`, `server.go` and `config.go`.

## Must have

None.

## Should have

None.

## Could have

### Servers of one type share a name

Two `http.Config`s in one app, such as the public and admin servers REFERENCE.md shows, both log `server_name: http`. Only the random `server_id` tells them apart. A `Name` field in `ServerConfig`, defaulting to the server type, would fix it, and would make `IServer.Name` redundant next to `ServerFactory.Name`. It touches every server package.

### `RunContext`

`Run` stops only on a signal or a failed server. A `RunContext(ctx)` would let tests and programs that embed an app stop it without signals.

### Typed `Env`

`Env` returns strings only. `EnvInt`, `EnvBool` and `EnvDuration`, or a documented `strconv` pattern, would save every application the same parsing code.

### Bounded wait for tracked goroutines

`Run` waits for `process.Go` goroutines without a limit. A stuck one keeps the process up until the orchestrator kills it.

### A `Serve` that returns nil on its own

A server that stops by itself without an error is not noticed. If every server does, the app idles until a signal.

### A named resource of the wrong type

`ResolveResource` and `boot:"name"` return `ErrResourceNotFound` when the resource exists with another type. A distinct error would be clearer.

### forgo-adapter's README ignores `Run`'s error

`forgo-adapter/README.md` still shows `app.Run()`. It compiles, but the example exits 0 on failure. Update it with the adapter's next change.

## Outside this package

### Worker servers start goroutines after an early `Terminate`

`ConsumerServer.Serve` and `ProducerServer.Serve` start their goroutines with `wg.Go`. When `Terminate` runs before `Serve` does, for example on a signal right after start, it waits on an empty `WaitGroup` and returns. `Serve` then starts subscribers and the relay on a canceled context, and nothing joins them before resources close. Listed in AGENTS.md "Known gaps". Fix it with the worker review.

## Performance

Measured on an Apple M5 (10 cores) with `go test . -run '^$' -bench LoadMemory`:

- Bootstrap reflection runs only during `Build`, `New` and `Init`. Nothing to optimize.
- Parallel `Load` from memory costs 55 to 70 ns/op. Decoding alone takes 41 ns/op in parallel. Most of the rest is `time.Now`, which does not scale on macOS and does on Linux.
- `Load` allocates the zero `T` to find `MemoryTTL` through a pointer (one allocation of `sizeof(T)`). Avoiding it needs a type cache. Not worth it next to the decode.

Memory layout: `binding` (56 B), `Bootstrap`, `App`, `LiveConfigs`, `LiveConfigType` (48 B), `ServerConfig` (72 B) and `liveConfigState` (64 B in either field order) have no padding to remove.
