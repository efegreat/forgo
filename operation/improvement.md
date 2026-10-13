# operation: open improvements

Findings from the pre-v1 review of `operation/` that are not applied yet. The applied ones are in the package code, `middleware.Recorder`, the HTTP, worker and CLI transports, REFERENCE.md and AGENTS.md.

## Must have

None open.

## Should have

None open.

## Could have

### `Error.Format` takes the name of `fmt.Formatter`

`Format(args ...any) Error` cannot coexist with `Format(fmt.State, rune)`, so `Error` can never implement `fmt.Formatter`, for example to print its code with `%+v`. `go vet` accepts it only because the first parameter is not a `fmt.State`. A name such as `Messagef` would free it. Renaming it breaks every application that formats a message.

### `Status` and `ErrorSeverity` have no `String` method

`%v` prints `4` instead of `not_found`, in tests, panics and debug output. A `String()` on each would also give log pipelines readable values if a status is ever logged.

### `StatusUnimplemented`

A transport-agnostic "not implemented" (HTTP 501, gRPC `Unimplemented`) is the one common status missing. Append it, so existing values keep their numbers.

### The fallback code `"x"`

Clients and alerts see `errCode: "x"` for every unmapped error. An exported `FallbackErrorCode` constant would let them refer to it, and a readable code such as `internal` would explain itself. Changing the value changes the wire contract.

### Nil `ErrorMapper` and `Map(nil)`

`(*ErrorMapper)(nil).Map(err)` panics, while the framework's other options treat nil as "no-op". `Map(nil)` returns a fallback error for no error. A nil mapper could map only through the chain and the fallback, and `Map` could document that `err` must not be nil.

### Extending a mapper

Setting `http.Config.ErrorMapper` replaces the HTTP defaults, and REFERENCE.md tells users to list them again. `(*ErrorMapper).With(mappers ...ErrorMapperFunc) *ErrorMapper`, which returns a new mapper that tries the given mappers first, would let a server add mappers to its defaults.

### The `Transactor` contract leaves two questions open

It does not say whether `Do` may call `fn` more than once, as transaction helpers that retry serialization failures do, or what `Do` does when `ctx` already carries a transaction. The worker runs the handler and the inbox update inside `fn`, so calling `fn` twice reruns the handler's side effects outside the database.

### Backoff for `Tolerance`

`RetryDelay` is fixed. Retrying an unavailable dependency usually wants exponential backoff with a cap, such as `RetryBackoff float64` and `RetryMaxDelay time.Duration`, with zero meaning a fixed delay.

`Tolerance{Retry: true}`, with neither a limit nor a delay, retries a failing operation in a tight loop. Its fields' docs and REFERENCE.md now warn about it. A minimum delay for unlimited attempts would remove the trap, but the "zero means unlimited" semantics were settled in the middleware review.

### Small inconsistencies

- `DefaultTolerance` is a mutable package variable, so one package can change every other package's default. A function or a documented "do not modify" would prevent that.
- `Tolerance.RetryAttempts` is a `uint`, while `ILocker.WaitLock` takes `attempts int`.
- `Status` is a `uint8` and `ErrorSeverity` an `int`.

## Performance

Measured with `go test ./operation -bench .` on an Apple M5:

- `Context.Value` costs about 4 ns for a key that is not in an empty `Args`, and 11 ns when `Args` holds values, since `Get` then takes the read lock. Each operation's `Args` is its own, so the lock is never contended. The `FromCtx` key check added no measurable cost.
- `FromCtx` takes 1.7 ns on the `*Context` a transport passes, and 3.9 ns through one derived context. Neither allocates.

Nothing in the package can be repacked to save memory. `Context` (88 bytes), `Lock` (24), `Tolerance` (64) and `Error` (64) have no padding to remove by reordering. `Error` would shrink to 56 bytes only if `ErrorSeverity` became a `uint8`, which changes a public type for no measurable gain.
