# logger: improvements

Findings from the pre-v1 review of `logger/`. Items marked **breaking** change public API, so they are now or never once v1 is tagged.

## Must have

None.

## Should have

None.

## Could have

### `FieldAdder` and `EventFields` make `logger` depend on `operation` (**breaking**)

They are the only reason `logger` imports `operation` (`logger/context.go:8`). `operation` can then never use `logger`, for example to log a `Tolerance` retry with the shared keys. Both types belong to the middlewares that take them; moving them to `middleware` makes `logger` a leaf package. The cost is a rename for users who write their own field functions.

### Users cannot add fields to `operation started`/`completed`

`Recorder` takes the logger from the context before it calls the rest of the chain (`middleware/recorder.go:26`), and server middlewares run inside it. A user middleware that derives a logger with a trace ID, tenant or user ID changes every log line below it, but not the two lines `Recorder` writes. Only the transport's `FieldAdder` reaches them, and the HTTP server passes none. An additive option, such as a `LogFields logger.FieldAdder` on `ServerConfig` that the base `Logger` middleware applies, makes OpenTelemetry trace correlation and similar fields possible without `DisableBaseMiddlewares`.

### Deterministic `StaticFields` order

`New` ranges over the map (`logger/config.go:70`), so the order of these fields differs from one process to the next. Sorting the keys costs nothing at runtime, since it happens once in `New`, and makes lines comparable and golden tests possible.

### Console output for development

`zerolog.ConsoleWriter` defaults to `time.Kitchen` (`3:04PM`), so console lines have no seconds. It also writes colors when the output is not a terminal, such as under `docker logs` or in CI, unless `NoColor` is set. Set `TimeFormat` to `15:04:05.000`, and turn colors off when `Output` is an `*os.File` that is not a character device (no dependency needed).

### `NewCtx` allocates twice

`NewCtx` copies the logger to the heap and wraps it in a `context.valueCtx` (`logger/context.go:30`): 2 allocations, 160 B, 34 ns. One context type that holds the logger by value and answers `Value(loggerKey)` takes 1 allocation, 128 B, 17 ns. `middleware.Logger` calls it once per operation, out of 3 allocations and 672 B in total; most of the rest is the 512 B buffer of zerolog's `With()`, which stays.

### Interoperate with `zerolog.Ctx`

`NewCtx` stores the logger under forgo's own key, so a library that reads `zerolog.Ctx(ctx)` (such as `hlog`) gets zerolog's disabled logger instead of the operation's logger. Storing it under zerolog's key as well, with `Logger.WithContext`, fixes that. zerolog's `WithContext` does not store a disabled logger when none is set yet, so `FromCtx` must keep its own key or handle that case.

### Default logger after parallel runs

`SetDefault`'s restore reinstates the default it replaced, and does nothing once a later `SetDefault` replaced its logger. That is exact for one `App`, and for CLIs run one after another. When CLIs run in parallel tests and finish out of order, the default ends up as the logger of a finished run, so later lines without a context go to its output, typically a test buffer. A list of installed loggers, where each restore removes its own and the newest remaining one becomes the default, is exact in every order.

### lumberjack lifecycle and options

- `New` never closes the `lumberjack.Logger`. Its file and its background pruning goroutine stay until the process exits. That is harmless for one `App`, but every `CLI.Run` in a test with `File` enabled leaks both.
- Two `New` calls with the same `Filename` produce two independent rotators on one file. The default name does not collide between a server and a CLI, but nothing detects an explicit shared name.
- lumberjack's `LocalTime` (backup names in local time instead of UTC) is not exposed.

### Hooks and runtime level

`Config` has no `Hooks []zerolog.Hook`, for example to count error lines as a metric. The level is fixed at `New`; `zerolog.SetGlobalLevel` can only raise it, since an event must pass both levels. Document that, since a live config that drives `SetGlobalLevel` is a natural way to change verbosity at runtime.

### A test for the key constants

A test that every key value is unique and differs from zerolog's own field names (`level`, `time`, `message`, `error`, `caller`, `stack`) and from the names pipelines reserve (`status`, `severity`) keeps the schema safe as keys are added.

### `App.Run` formats a field into the message (outside `logger/`)

`server termination signaled, waiting: %s` (`app.go:200`) puts the wait into the message with `Msgf`, and `app terminating: %v` (`app.go:117`) the signal, so neither can be queried. Log them as fields, under keys in `constant.go`, and keep the messages constant.
