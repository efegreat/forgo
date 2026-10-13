# process: improvements

Findings from the pre-v1 review of `process/`.

## Must have

None.

## Should have

None.

## Could have

### Recover panics in tracked goroutines

A panic in a `process.Go` goroutine crashes the application, and it is printed on stderr instead of the app's log. HTTP handlers, jobs and message handlers are all recovered. A `GoCtx(ctx, fn func(context.Context))` that recovers and logs with `logger.FromCtx(ctx)`, keeping the stack like `middleware.PanicError`, would make background work as safe as an operation. It needs a context, so it would be a new function rather than a change to `Go`.

### Bound the wait at shutdown

`WaitGoroutines` has no timeout, and neither does `App`'s call to it (`app.go`). A stuck background goroutine holds the app until Kubernetes kills it, and resources are never closed. A `WaitGoroutinesContext(ctx) error`, used by `App` with its own timeout, would bound it.

### Names that tell the runners apart

`Runner` (never waits for a slot, tracked by `WaitGoroutines`) and `ConcurrentRunner` (waits for a slot, joined by its own `Wait`) do not say what separates them, and `DoAsync`/`TryAsync` are not Go idiom. Names like `Go`/`GoOrRun` would read better, but the change would only be cosmetic.

## Performance

Nothing here is on a request path: `process` runs per background task or per message. `go test ./process -bench .` on an Apple M-series machine:

| Benchmark | Time | Allocations |
| --- | --- | --- |
| `Go` | 217 ns | 1 (24 B) |
| `ChannelSemaphore` `Acquire` + `Release` | 13 ns | 0 |
| `ChannelSemaphore` `WaitAcquire` + `Release` | 21 ns | 0 |
| `Runner.TryAsync` | 275 ns | 1 |
| `ConcurrentRunner.Run` | 405 ns | 2 (48 B) |

The tracker behind `Go` counts with an atomic and takes its mutex only when the count drops to zero. The 8 ns that `WaitAcquire` adds over `Acquire` is the check of the context before taking a slot.

None of the structs can be repacked to save memory: `ChannelSemaphore` holds one pointer, `Runner` one interface, `ConcurrentRunner` an interface and a `sync.WaitGroup`, and the tracker an atomic, a mutex and a condition, with no padding between them.
