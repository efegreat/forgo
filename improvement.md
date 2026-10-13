# forgo (root package): open improvements

Findings from the pre-v1 review of the root package (`app.go`, `bootstrap.go`, `server.go`, `config.go`) that are not applied yet. The applied ones (every must have and should have) are in the package code, REFERENCE.md, README.md and AGENTS.md:

- `App.Run` returns an error, like `cli.CLI.Run`. A failed `Serve` used to stop the app with exit code 0, so restart-on-failure policies missed it. Start failures used to panic, and resource close errors were dropped. Every failure is now logged with `err_severity` and returned.
- `LiveConfig.Default` must build its maps, slices and pointers on every call. `Load` decodes into them, so a shared one received the stored values and raced under concurrent loads (verified with `-race`). REFERENCE.md no longer says callers may always modify a loaded value freely.
- `App` applies the default `TermTimeout` to a server whose `Term()` returns zero or less. Such a server used to be killed at once.
- Termination waits for `Serve` to return as well as `Terminate`, within `TermTimeout`. Once termination starts, signals are no longer caught, so a second Ctrl+C ends the process.
- Bootstrap recovers panics in resource factories and resources, so earlier resources are still closed and one panicking `Close` does not skip the others.
- `ResolveMiddlewares` rejects nil factories and nil middlewares, which used to panic in `New` or on the first operation. `Run` rejects an empty or nil server list and a `New` that returns nil.
- Bootstrap errors name things once: `bootstrap: provider 0 (yourapp/user.NewService): argument 0 (user.Repository): missing dependency user.Repository`.
- `Resolve` and `ResolveResource` return a value of an unnamed type for a named `T`, such as a `func(int) int` for `type Fn func(int) int`, instead of failing an assertion.
- `LiveConfigs.Load` reads memory without a lock: 233 → about 60 ns/op for parallel loads on 10 cores.
- `TermSig` is an `atomic.Bool` instead of a mutex and a bool (40 → 16 bytes).
- A package comment, and doc comments for every exported identifier. `IServer` documents its lifecycle contract.
- `bootstrap.go` follows the module's style (1225 → 693 lines), errors follow the `component: …` convention, and `WithLogger` no longer shadows the `logger` package.
- Tests for the bootstrap, `App` (including a real second SIGTERM in a subprocess) and `server.go`. Coverage 43.5% → 98.1%.

## Must have

None open.

## Should have

None open.

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
- Parallel `Load` from memory: 233 ns/op with the shared mutex, about 106 with a read lock, 55 to 70 lock-free. Decoding alone takes 41 ns/op in parallel. What remains is mostly `time.Now`, which does not scale on macOS and does on Linux.
- `Load` allocates the zero `T` to find `MemoryTTL` through a pointer (one allocation of `sizeof(T)`). Avoiding it needs a type cache. Not worth it next to the decode.

Memory layout: `binding` (56 B), `Bootstrap`, `App`, `LiveConfigs`, `LiveConfigType` (48 B), `ServerConfig` (72 B) and `liveConfigState` (64 B in either field order) have no padding to remove. `TermSig` went from 40 to 16 bytes.
