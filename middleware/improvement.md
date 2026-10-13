# middleware: improvements

Findings from the pre-v1 review of `middleware/`.

## Must have

None.

## Should have

None.

## Could have

### Error messages without a component prefix

`"failed to acquire lock: %w"` (`middleware/locker.go`) and the message of `ErrNoLocker` have no component prefix, and the first one does not name the key. `"locker: acquire %s: %w"` and `"locker: no locker configured"` would follow the convention.

### Log retried attempts

A cron job that fails twice and then succeeds logs only `operation completed`. The failures leave no trace unless the job sets `OnRetry`. A warn line per retried attempt, such as `operation attempt failed, retrying` with a new `attempt` log key, would make flaky jobs visible. The run would still log one `started`/`completed` pair, as documented.

### Keep the outer context of wrapped `operation.Error`s on HTTP

`Recorder` logs the operation's error itself, and `operation.Error.Error()` includes the cause, so `fmt.Errorf("charge order 42: %w", opErr.Wrap(cause))` is logged whole on cron, the CLI and the worker. A panic whose value is an `operation.Error` keeps its stack there too.

HTTP still loses `charge order 42`: the terminal handler returns the `operation.Error` that `ErrorMapper.Map` found inside the error, as the serializer contract asks (`server/http/operation.go`). Passing the original error on when it already holds that `operation.Error` would keep it. Fix it with the `server/http` review.

### Name the key in the conflict log

`operation already running, skipping` does not say which lock was held. A `lock_key` field (a new constant in `logger/constant.go`) would make the line useful when a job computes its key, as the worker does per message.

### A transport-agnostic `Timeout` middleware

HTTP routes and cron jobs have no operation timeout, and REFERENCE.md tells cron users to add their own. Only the worker has one, per handler. `middleware.Timeout(d)` would run `next` on a context bounded by `d` and then restore `ctx.Context`, as `Tolerance` does for `RetryTimeout`.

### Customizing a base chain

Base chains are all or nothing (`DisableBaseMiddlewares`). Changing one piece, such as the ID format or the `Logger` fields on HTTP, means rebuilding the whole chain. Every piece is exported, but REFERENCE.md has no recipe for it. A short example per server, or an `IDWith(func() string)` constructor, would cover the common cases.

### Retry waits outlive shutdown (cron)

Cron runs jobs on `context.WithoutCancel`. Its `Terminate` ignores its deadline, and `Kill` is a no-op (`server/cron/server.go`). `Tolerance` stops waiting when its context ends, but cron gives it a context that never ends. A failing job with `DefaultTolerance` therefore holds shutdown for about 20 s of retry waits, past `TermTimeout`. Fix it with the `server/cron` review.

## Performance

`go test ./middleware -bench BaseChain` runs the transport-agnostic part of every base chain. On an Apple M-series machine it takes about 560 ns, with 7 allocations (880 B) for HTTP, worker and CLI, and 8 for cron. Two of those allocations are the benchmark's own `operation.Context`. The one extra allocation on cron is the `context.WithoutCancel` that `Locker` uses for `Unlock`.

Two costs are inherent and need no change. Generating a UUIDv7 in `ID` takes about 45% of the chain, since it reads `crypto/rand` and takes google/uuid's time mutex. `Logger` costs a 500-byte buffer from zerolog's `With()`.

A successful `Tolerance` run allocates nothing, and a test (`TestToleranceAllocations`) checks this. None of these structs can be repacked to save memory: the package declares only `PanicError`, and `operation.Lock`, `operation.Tolerance` and `operation.Context` have no padding to remove.
