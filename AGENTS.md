# AGENTS.md

Guide for coding agents and contributors working **on forgo itself**. For how to *use* forgo, see [REFERENCE.md](REFERENCE.md). [README.md](README.md) is the public entry point.

## What forgo is

A Go framework that implements the technical scaffold every backend needs (dependency wiring, server lifecycle, operations and middleware, logging, CLI, migrations, OpenAPI), so applications only write business code. Module path: `github.com/efegreat/forgo`. Go 1.27, and the code relies on **generic methods** (`func (r Resolver) Resolve[T any]()`).

## Design principles

Every change must serve these four goals. When they conflict, simplicity for the user wins, unless it costs runtime work.

1. **Simplicity.** Users declare configuration and hand over constructors. New features are exposed as config structs whose zero values work. `Sanitize()` fills defaults. No imperative registration APIs, no required boilerplate.
2. **Speed.** Everything is resolved before serving. Reflection, `Resolver` calls and handler or chain construction happen only in `Bootstrap.Build`, `ServerFactory.New` and `IServer.Init`. The request path runs pre-built closures only. The bootstrap is sealed before `Serve`, so it never becomes a runtime service locator.
3. **Abstraction.** Every unit of work is an `operation`. Transports translate their input into `*operation.Context` (`Type`, `Name`, `ID`, `Args`, `State`) and run an `operation.Handler` chain. Cross-cutting behavior is an `operation.Middleware` that works on every transport. Read transport `State` only when unavoidable.
4. **Scalability.** Everything is replaceable behind interfaces: `IServer`, `ServerFactory`, `Resource`, `ResourceFactory`, `transport.ISerializer`, binders, `operation.ILocker`, `operation.Transactor`, `worker.IBroker`, outbox/inbox storages, `LiveConfigStore`, `LiveConfigCache`. Core depends on interfaces only.

## Repository map

| Path | Responsibility |
| --- | --- |
| `app.go` | `App`: bootstrap → create and init servers → seal → serve → parallel termination → `process.WaitGoroutines` → close resources. `Run` logs and returns every failure, and never exits or panics. |
| `bootstrap.go` | `Bootstrap`, `Resolver` (`Resolve`, `ResolveResource`, `Call`), `Resource`/`ResourceFactory`, `MiddlewareFactory`, `MiddlewareAs`, `ResolveMiddlewares`. |
| `server.go` | `IServer`, `ServerFactory`, `TermSig`, `TermConfig`, `ServerConfig` (embedded by every server config). |
| `config.go` | `Env(key, default)`, and live configs: `LiveConfig[T]` (`Name`, `Default`), `LiveConfigStore` (versioned JSON documents, the source of truth), `LiveConfigCache` (shared cache, newest version wins), `LiveConfigMemoryTTL` (optional per-instance memory), `LiveConfigs` (`Load[T]`, `Save`, and `LoadStored[T]`/`SaveChanged` for admins), `LiveConfigCatalog`/`LiveConfigType`/`LiveConfigOf` (the configs admin endpoints expose, behind `any`), `ErrLiveConfigConflict`, `ErrLiveConfigNotCached`, `ErrLiveConfigInvalid`. |
| `operation/` | `Context` (and `FromCtx`, which finds it in derived contexts), `Handler`, `Middleware`, `Chain`, `Error`, `Status`, `ErrorSeverity`, `ErrorMapper`, `Lock`/`ILocker`, `Tolerance`, `Transactor`. Must stay transport-agnostic and dependency-light. |
| `middleware/` | Generic middlewares: `ID`, `Logger`, `Recorder`, `Recovery` (panics become `*PanicError`, which keeps the stack), `Locker`, `Tolerance` (its own retry loop), `Guard` (`ErrRejected` without a reject function). No transport imports. |
| `server/http/` | HTTP server (std `net/http` + `ServeMux` patterns), route and handler factories, bingo binder, JSON envelope serializer, error mappers, request/response recording. `liveconfig.go` holds the live config admin handlers (`LiveConfigsHandler`, `SetLiveConfigHandler`). `middleware.go` holds `ErrorResponder` and the opt-in `CORS`, `RateLimit`/`RateLimitWith` and `JWTAuth` middlewares, backed by `ratelimit.go` (`IRateLimiter`, in-memory GCRA limiter, `ClientIP`) and `jwt.go` (key lookup and key rules around golang-jwt v5 verification, `JWTKeys`, `JWKS`). `config.go` also holds `MiddlewareAs[Params]`, a `forgo.MiddlewareAs` whose `Params` documents the request parameters a middleware reads, and `MiddlewareContractOf`, which reads `Params` back by calling the factory against a private, empty bootstrap. |
| `server/cron/` | Cron server (robfig/cron v3), `IJob`, `cron.As`. Schedules are parsed in `New`. `Terminate` only stops scheduling and waits for running jobs, and jobs run on a context that only `Kill` cancels. |
| `server/worker/` | Consumer server (`ConsumerConfig`: inbox, idempotency, attempts, transactor, `ErrNoRetry`) and producer server (`ProducerConfig`: outbox relay); broker, publisher, subscriber, delivery and storage contracts; `Message`. `lifecycle.go` holds what both servers share to start their goroutines in `Serve` (never after `Terminate`), join them in `Terminate` and cancel the work in flight in `Kill`. |
| `server/grpc/` | gRPC server on grpc-go. Handlers take and return business structs, which the server converts from and to the proto messages. `config.go` checks every method against its generated `ServiceDesc` and the proto registry (kind) and builds forgo's own descriptor, `binding.go` the conversions between messages and structs, planned once in `New` from the registry's descriptors (field matching by tag or name, scalars, enums, well-known types, metadata fields), `handler.go` the factories (`HandlerFor`/`HandlerAs` and the `ServerStream`/`ClientStream`/`BidiStream` `For`/`As` pairs) and terminals, `operation.go` `State` and the stream wrapper that records failed receives and sends, `errors.go` the `Responder`, the status conversion, the default validator and mappers, `interceptor.go` the adapters that run grpc-go interceptors as middlewares, `logging.go` the masked metadata and message fields, `messagejson.go` the JSON of logged messages, `health.go` `HealthService`, `reflection.go` the reflection services, `status.go` the `operation.Status` ↔ code maps. |
| `cli/` | `CLI` runner (`New`, `Run`, options including `WithArgs`), `Command`/`CommandFactory`, `cli.As`, `ErrNoCommand`/`ErrUnknownCommand`. |
| `commands/migration/` | `migrate` command: `Factory(CommandConfig)` builds it from config, `Migrator` plans and applies sql-migrate sources under a session advisory lock (`Locker.TryLock`, MySQL/Postgres lockers), `Creator` writes new files. The dialect is detected from the driver's package path, so the package imports no driver. |
| `commands/openapi/` | OpenAPI 3.1 generator and command, driven by `http.Config`, serializer probing and the `Params` of `http.MiddlewareAs` middlewares. Routes with `RouteConfig.DisableAPIDoc` are left out. Paths are checked against `ServeMux`, request fields are documented from every source bingo binds them from, `WithSchema` describes types that cannot be inferred, and `WithDocument` hooks add servers, security and tags. Golden files in `testdata/` lock the output (`go test ./commands/openapi -run TestGolden -update` rewrites them). |
| `logger/` | `logger.Config` (zerolog + lumberjack: `Output`, `Console`, `File`), `NewCtx`/`FromCtx`, `SetDefault` (the logger of contexts without one), `FieldAdder`/`EventFields`, and **all** log key constants. Importing it sets `zerolog.TimeFieldFormat` once, in `init`. |
| `process/` | `process.Go`/`WaitGoroutines` (process-wide goroutine tracker), `Runner` (tracked, never waits for a slot), `ConcurrentRunner` (waits for a slot, joined by its own `Wait`), `ISemaphore`/`ChannelSemaphore`. |
| `transport/` | `ISerializer`, connection observers (`IConnObserver`, and `IConnIOObserver` for read and write errors) and the `Listener` that reports to them. It wraps connections only for an `IConnIOObserver`, and that wrapper must keep what net/http relies on (`CloseWrite`) and ignore net/http's own cancellations. Standard library only. |
| `types/` | Concurrency-safe `Map` (copy-on-write for shared static args; a nil `*Map` reads like a nil Go map) and `JSONMap`, which encodes through `encoding/json` and as a database value. `JSONMap.Value` returns a `string`, never a `[]byte`, which MySQL JSON columns reject under `interpolateParams`. The OpenAPI generator documents `JSONMap[V]` as an object of `V`, found through the result type of its `Clone`. |
| `internal/headerlog/` | Reflection-free JSON encoding of logged headers, byte-identical to `encoding/json`. Shared by the HTTP and worker recorders. |
| `redact/` | Masking of `SensitiveFields` in logs: `Fields` (a name set matched like `strings.EqualFold`), the `CredentialHeaders` every transport masks, allocation-free JSON masking and compaction (`LogJSON`, and `AppendJSON` over a string or `[]byte`, which follow nesting down to `MaxJSONDepth`), and binding errors without the input they quote. Shared by the HTTP and worker recorders and binders, and public so that forgo-adapter's SQL query log masks arguments with the same rules. |
| `.github/` | The CI workflow (lint, tests, fuzzing, integration tests, govulncheck, releases on `v*` tags), Dependabot, and the issue and pull request templates. |

**Adapters** (concrete resources and providers: SQL databases, Redis, RabbitMQ, Google Cloud Pub/Sub, Redis Streams, outbox/inbox storages, live config stores and cache, the Redis locker and rate limiter) belong to the adapter library, [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/AGENTS.md), a separate module, `github.com/efegreat/forgo-adapter`. Core packages must never import adapters, and no adapter dependency (a driver, broker or cache client) may enter forgo's `go.mod`. Adapters import core and implement its interfaces. A resource's `Value()` must return the exact type providers are expected to request. When you change a contract that adapters implement (`worker.IBroker`, `IPublisher`, `ISubscriber`, `IDelivery`, `InboxStorage`, `OutboxStorage`, `operation.ILocker`, `http.IRateLimiter`, `forgo.LiveConfigStore`, `forgo.LiveConfigCache`), update forgo-adapter in the same change set and run its tests.

## Architecture invariants

Do not break these without an explicit decision from the maintainer.

### Lifecycle

- Order: create the logger and install it with `logger.SetDefault` → validate the server list → `Build` (resources, then providers) → for each server `New(resolver)` then `Init(ctx)` → `Seal` → `Serve` in goroutines → on signal or first `Serve` error, stop catching signals and terminate all servers in parallel (`TermSig.Signal` + `Wait` → `Terminate(ctx with the server's logger and TermTimeout)` → `Kill` unless `Terminate` returned nil and `Serve` returned by the deadline) → `process.WaitGoroutines()` → `Bootstrap.Close` (reverse order) → return the failures joined.
- `App.Run` returns errors like `CLI.Run`, so `main` picks the exit code: start failures, the first `Serve` error, termination failures and resource close errors, each logged with `err_severity`. It returns `nil` only after a clean stop on a signal. A failed start terminates no server, which is why `Init` must not start goroutines.
- `Init` receives a context whose logger already carries `app_name`, `app_id`, `server_name` and `server_id`. Servers store this context and derive everything from it.
- `Serve` blocks. It returns `nil` on graceful stop, and a non-nil error stops the whole app. `Terminate` must make `Serve` return and join every goroutine the server started. `App` applies the default `TermTimeout` to servers whose `Term()` returns zero or less.

### Bootstrap

- No topological sorting. Providers are invoked in declaration order and may only depend on earlier bindings. This is deliberate: keep it.
- Matching is `AssignableTo` over all bindings: exactly 1 match → inject, 0 → `ErrMissingDependency`, 2 or more → `ErrAmbiguousDependency`. Never add "best match" heuristics.
- Providers bind under their declared result type. Resources bind under the dynamic type of `Value()`. Duplicate provider result types are rejected up front.
- `boot:"name"` tags resolve **resources only**, by `Resource.Name()`.
- `Resolver.Call` constructs entry points (handlers, middlewares, jobs, commands) and **never registers** them.
- Constructors are invoked through `invoke` (panics → errors), and panics of resource factories and resources are recovered too. A nil result is an error. Failed builds roll back the resources created so far.

### Server config pattern

Every server follows the same shape. Copy it for new servers:

- A `Config` value type embeds `forgo.ServerConfig` and implements `ServerFactory` with **value receivers**: `Name()` and `New(r forgo.Resolver)`.
- The config is the server's whole API. The server type is **unexported** (`server`, `consumerServer`), and `New` returns it as a `forgo.IServer`, so applications never name it.
- `Sanitize()` is a value receiver that returns a copy with defaults applied. Defaults are named constants or literals inside `Sanitize`, and zero always means "use default".
- `New`: `Sanitize()` → `ServerConfig.ResolveMiddlewares(r)` → resolve every handler, job or command through its factory → store resolved values in **unexported** fields (`routes`, `jobs`, `resolvedSubscribers`) → return the server. Optional dependencies, such as `operation.ILocker`, are resolved with `r.Resolve[T]()`, ignoring `forgo.ErrMissingDependency`. When the configuration needs one, for example a `Lock` with `Lock.Enabled()` and base middlewares on, `New` fails with an error naming the job, command or handler. Never let a missing optional dependency surface at runtime.
- Entry-point factories follow two idioms:
  - `XAs(constructor any)` → `func(r forgo.Resolver) (T, error) { return r.Call[T](constructor) }` (`forgo.MiddlewareAs`, `cron.As`, `cli.As`, `http.HandlerAs`, `worker.HandlerAs`).
  - `HandlerFor(methodExpr)` resolves the receiver with `r.Resolve[S]()` and closes over it.
- Handler chains are built **once**, in `New` or `Init`. Per-operation work is limited to allocating the `operation.Context`, its `Args` wrapper and transport state. Static args are cloned once in `New` and shared through `types.NewMapCopyOnWrite`, never copied per operation.

### Middleware order

Chain order is **base → server → narrower scopes (subscriber) → route/handler → terminal handler**, and base middlewares are skipped when `DisableBaseMiddlewares` is set.

| Server | Base chain |
| --- | --- |
| HTTP | `ID` → `Logger` → `Recorder` → `ErrorResponder` → `Recovery` |
| gRPC | `ID` → `Logger` → `Recorder` → `Responder` → `Recovery` |
| Cron | `ID` → `Logger` → `Locker` → `Recorder` → `Tolerance` → `Recovery` |
| Worker consumer | `ID` → `Logger(msg_id)` → `Locker` → `Recorder` → `Recovery(retry/reject)` → `idempotency` |
| CLI | `ID` → `Logger` → `Locker` → `Recorder` → `Recovery` |

Opt-in HTTP middlewares (`CORS`, `RateLimit`, `JWTAuth`) are never part of a base chain, and read `*http.State`, so they serve HTTP only. Their constructors take a config and return a constructor for `forgo.MiddlewareAs`, so config errors fail in `New` and dependencies (such as `IRateLimiter` for `RateLimitWith`) come from bootstrap.

Positions matter. `Recorder` sits outside `Recovery` so panics are logged as failures. `Locker` sits outside `Tolerance` so the lock spans retries, and outside `Recorder` so a run skipped for a held lock logs no operation. That is why `Locker` logs its own acquisition failures: a cron run has nobody else to log them. HTTP error responses are written by the terminal handler wrapper. Any error that reaches `ErrorResponder` with nothing written yet, such as a middleware rejection or the `*PanicError` of `Recovery`, is written there, inside `Recorder` so the logged response is the real one. The transport repeats that check after the chain for `DisableBaseMiddlewares`. The HTTP server writes a response at most once: every answer goes through `respond`/`respondError`, which leave a written response alone, so a handler can write its own through `State.Writer`.

### Operations and errors

- Transport operation types are constants: `http_handler`, `grpc_handler`, `cron_job`, `message_handler`, `command`. Each transport defines its own `State` type, and its own unexported type for the `Args` keys it reads (`http.ArgKeyRecorderDisabled`).
- Business errors are `operation.Error`. `Message` is public and is all a transport sends; never send `Error()`. The wrapped error is private: `Error()` appends it to `Message`, so logs and wrapping errors keep the whole chain, and `Recorder` logs the operation's error itself. A `Message` must therefore not repeat the text of the error it wraps. `Is` matches by `Code`. `Status` is semantic; transports map it (`server/http/status.go`, `server/grpc/status.go`). `Severity` decides the log level in `Recorder` (Light → warn, otherwise error).
- Terminal handlers return the error they answered, so `Recorder` logs the code and severity the client got. On HTTP that is the handler's error itself when it holds an `operation.Error`, so the log keeps what is wrapped around it, and the mapped error otherwise, with the original as its cause.
- `Recovery` turns a panic into a `*middleware.PanicError`, which holds the stack. A transport hook that maps the panic to another error keeps the `*PanicError` as its cause, so the stack is always logged. HTTP needs no hook: `ErrorResponder` answers the `*PanicError` like any error and the operation fails with it.
- `operation.Transactor.Do(ctx, fn)` calls `fn` with a context carrying the transaction. The worker copies the `*operation.Context` onto that context, so handlers always receive an `*operation.Context`.
- Retries never wait in the consumer, so they hold no concurrency slot. `IDelivery.Retry` receives the failed attempt from the inbox (0 before it was counted), and the broker adapter turns it into a delay from its own configuration, such as forgo-adapter's RabbitMQ retry queues. Without one, the broker redelivers at once.
- The consumer settles every delivery exactly once. `State.Delivery` passes only the first `Complete`, `Retry` or `Reject` to the broker. After the chain returns, the consumer settles a delivery the chain left unsettled: it acknowledges on `nil` and retries on an error. This happens outside the chain, so it also covers the middlewares outside `Recorder` and `DisableBaseMiddlewares`. An error path may return without settling and leave the delivery to the consumer.
- The idempotency guard settles a redelivery like the inbox status says: *completed* is acknowledged, *failed* and *rejected* are rejected again. The inbox is marked before the broker rejects, and it keeps no payload, so acknowledging a message whose rejection failed (a dead-letter copy the broker could not store, a channel lost before the nack) would drop it.

### Live configs

- `Load[T]` decodes the stored document over `Default()`, never the other way round, so fields added later get their default and an admin's zero values are kept.
- `Load` reads the cache, then the store, then returns `Default()`. Cache failures are logged at `SeverityModerate` and fall through to the store; only store failures are returned. `Load` never writes the shared cache and never waits for another caller. Reads from memory take no lock (`sync.Map` of per-config memories holding an immutable `liveConfigState` behind an `atomic.Pointer`), and the per-config mutex that orders `remember` and `saved` is never held around I/O.
- Memory is opt-in per config through the optional `LiveConfigMemoryTTL`, looked up on a pointer to the zero `T` so either receiver works. It holds documents, not decoded values, so every `Load` returns a value of its own. `Load` remembers only documents read successfully, and only when no `Save` of the name went through this instance since its `recall` (`liveConfigState.saves`). `Save` keeps its document in memory only when the cache accepted it, and clears the memory otherwise.
- Only `Save` writes the cache, after the store accepted the versioned write, on `context.WithoutCancel`. Cache entries never expire, so the cache's version guard (keep the newest version) is what keeps out-of-order saves from caching an older version. A failed cache write after a stored save is `ErrLiveConfigNotCached`, never only logged.
- Admin reads and writes go to the store (`LoadStored`, `SaveChanged`), never the cache or memory. `SaveChanged` compares JSON encodings, skips the version check when nothing changed, and then re-caches the stored version, so resending a change that got `ErrLiveConfigNotCached` repairs the cache. `LiveConfigType.Decode` rejects unknown fields and trailing data, unlike `Load`.
- The HTTP admin handlers keep their logic in core, behind `LiveConfigType`, so other transports can reuse it. `SetLiveConfigHandler` validates through the server's `IBinder` by binding the complete JSON encoding of the decoded config into a second, discarded value: binders decode into a zeroed value, so they cannot decode over `Default()` themselves.
- `LiveConfigStore` and `LiveConfigCache` move JSON bytes only. Encoding and decoding stay in `LiveConfigs`, so stores and caches never see the config types. Their `Get` methods differ in shape (version or found), so no type implements both and bootstrap never finds them ambiguous.

### gRPC

- Business code imports no generated type. A handler's request and response are structs of its own, converted from and to the method's proto messages by a `binding` that `New` plans once from the message descriptor in the proto registry. A call only runs the plan: no lookup, no registry access, no tag parsing. A proto message type is passed as it is, the escape hatch for hand-written layers and hot paths.
- A binding never drops data silently: `New` fails on a request message field without a struct field (a blank field tagged `grpc:"name"` ignores it) and on an exported response struct field without a message field (`grpc:"-"` leaves it out). Conversions that can lose data, such as a number out of range, fail per value: `fieldError`, mapped by `MapBinding` to `InvalidArgument`, for requests, and `invalid_response` for responses.
- Field names match like the HTTP binder's tags (`json`, `uri`, `query`, and `header` for metadata), so one struct binds over both transports. The default validator is go-playground's, as over HTTP, and `New` validates an empty value of every request type, nested structs included, so that go-playground panics on an unknown tag at startup rather than on the first call. The panic is not recovered, like other library panics on bad configuration.
- forgo builds its own `grpc.ServiceDesc` per service, from the configured methods only, and registers it with a nil implementation. The generated handlers never run, and every other method reaches the `unimplemented` fallback through `grpc.UnknownServiceHandler`. grpc-go exits the process on a service registered twice, so `New` rejects duplicates.
- Every method, unary included, is registered as a `StreamDesc`. The `Responder` sends the one response of a unary or client-streaming method, `State.Response`, after the chain returns, so that middlewares can replace it or answer without calling `next`, and inside the `Recorder`, so a failed send is logged. grpc-go still enforces one message each way from the flags. `TestSendFailed` fails if that changes.
- The request of a unary or server-streaming method is received before the chain, for middlewares and the request log, and converted and validated by the terminal.
- grpc-go writes a status itself as soon as `RecvMsg` or `SendMsg` fails, and only the first status is sent. The stream wrapper records that failure in `State` (first one only, atomically, since bidi handlers receive and send from two goroutines), and nothing answers the call again. The operation fails with an error of the code grpc-go sent even when the handler ignored it, and the transport returns that status to grpc-go, which sends it when the failure was a middleware's stream wrapper's own, and stats handlers see it too.
- The `Responder` chooses the status, in `State`, and the transport returns it after the chain. Without base middlewares the transport calls the same `respond` after the chain, which does nothing for a call already answered. `toStatus` never sends `Error()`. A status that `MapStatus` mapped is sent as it is, through the unexported `statusCause`.
- grpc-go interceptors run only as middlewares, inside the chain, through `UnaryInterceptor` and `StreamInterceptor`; there is no option to configure them outside it. The adapters give an interceptor the operation's own context (`op.Context`), never the operation, and make the context it passes on the operation's until its handler returns: a context derived from the operation and stored back into it would look itself up forever. `StatsHandlers` cover tracing and metrics.
- grpc-go's `GracefulStop` waits for every handler, even one whose client canceled, so the server keeps no `WaitGroup` of its own. `TestTerminateWaitsForCanceledCall` fails if that changes. Long-lived streams end through `State.Terminating`.
- An `IConnIOObserver` is wrapped so that it is not told about resets (`ECONNRESET`, `EPIPE`) once `Terminate` started: a grpc-go client closes an idle connection on the GOAWAY of `GracefulStop` before reading the PING that follows, and its system resets the connection.
- Tests that check the log line, or a stats handler event, of a call grpc-go answered early must wait for it (`harness.line` does), since the client can get that status before the operation ends. The binding tests register `forgotest/users.proto` from a text descriptor as `dynamicpb` types, so conversions are tested on messages that are not generated code too.

### Logging

- Use `logger.FromCtx(ctx)`. Never create ad-hoc loggers on the request path. The returned logger is shared and must not be mutated: derive one with `With()` and store it with `logger.NewCtx`.
- Every log key lives in `logger/constant.go`. Add new keys there and never inline string keys. Keys are the log schema users query and alert on: never rename one outside a major version, list new ones in REFERENCE.md "Log keys", and avoid values log pipelines read as the level of a line (`level`, `status`, `severity`).
- Never write zerolog globals from `Config.New` or at runtime: zerolog reads them on every event. `logger` sets `TimeFieldFormat` once, in `init`. The default logger is swapped atomically through `logger.SetDefault`, which `App.Run` and `CLI.Run` call with their logger.
- A CLI logs to stderr unless `logger.Config.Output` is set, because commands print their results to stdout.
- Error logs carry `Int(logger.ErrorSeverityLogKey, int(operation.SeverityX))`.
- Never log secrets. Request and message recorders mask the `SensitiveFields` of their config in headers, query, path, JSON bodies and binding errors (and always mask `redact.CredentialHeaders`), and cap logged bodies and headers (32 KiB). Masking is built before serving and is skipped entirely while `SensitiveFields` is empty.
- Log header maps with `headerlog.Log` and a transport-specific `ValueAppender` that does the masking, never with `json.Marshal`. Log JSON bodies with `redact.Fields.LogJSON`, which compacts them: a log line must never hold a newline, since log shippers split on it.
- The gRPC server logs a message with `appendMessageJSON`, which writes in one pass what `LogJSON` makes of `protojson`'s encoding (`UseProtoNames`), byte for byte, and fails where `protojson` fails. It hands the well-known types with a JSON form of their own (other than wrappers, `Timestamp` and `Duration`) and messages with extension ranges to `protojson` and `AppendJSON`, and messages nested deeper than `redact.MaxJSONDepth` back to `protojson` and `LogJSON`. `TestMessageJSON` and `FuzzMessageJSON` compare the two, so a protobuf upgrade that changes `protojson`'s output fails them.
- `Recorder` runs `EventFields` only for events that are logged, so the operation must never rely on their side effects.

### Goroutines

- Work that must finish before the process exits uses `process.Go`, which `App` and `CLI` wait on before closing resources.
- `process.Go` may run while `WaitGoroutines` waits, for example from a handler of a killed server, and `WaitGoroutines` waits for it too. That is why the tracker is not a `sync.WaitGroup`, which forbids it and panics.
- Servers own their internal goroutines (`sync.WaitGroup.Go`) and join them in `Terminate`.
- `context.WithoutCancel` is used where operations must not be cut by a parent cancellation (cron jobs, message handling, HTTP `BaseContext`). Cron and the worker derive a context from it that only `Kill` cancels, so that a killed server still stops its jobs, message handlers and publishes.

## Coding conventions

- Match the surrounding code. Doc comments on exported identifiers explain *why* and the contract, not the obvious.
- Errors: prefix with the component (`bootstrap: …`, `outbox: …`, `openapi: …`), wrap with `%w`, and declare sentinel errors as package-level `var`s. Use `errors.Join` for cleanup errors.
- Prefer the standard library. Current dependencies: zerolog, lumberjack, robfig/cron, bingo, go-playground/validator, uuid, xid, sql-migrate, lib/pq and go-sql-driver/mysql (only in the migration integration test), grpc-go (the gRPC server, its health and reflection services), protobuf (`protojson`, `protoregistry`, and `protoreflect` for the struct conversions) and genproto's `errdetails` (the `ErrorInfo` of gRPC errors), golang-jwt/jwt/v5 (JWT verification in `JWTAuth`; security-critical parsing is not reimplemented). Do not add a dependency without a strong reason, and never add a web framework (the HTTP server was moved off Gin to `net/http` on purpose).
- Configs are plain values that can be declared as package-level `var`s. Avoid pointers and constructors for configuration.
- Interfaces are named with an `I` prefix for framework contracts (`IServer`, `IJob`, `IBroker`, `IBinder`) or by role where that reads better (`Resource`, `Transactor`, `Runner`).
- Tests: table-driven where possible, `go-sqlmock` for SQL, `httptest` for HTTP. Keep tests passing under `-race`.

## Workflow

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
```

CI (`.github/workflows/ci.yml`) runs these on every push and pull request, plus `go mod tidy -diff`, govulncheck, `go test` without the race detector (the allocation tests skip under it), a 30-second run of every fuzz target and the migration integration tests against PostgreSQL and MySQL. A new fuzz target needs a row in the workflow's fuzz matrix. Silence an intentional staticcheck finding, such as a test that panics on purpose, with `//lint:ignore <check> <reason>`.

When you change behavior:

1. Update **[REFERENCE.md](REFERENCE.md)** in the same change whenever public API, defaults, config fields, middleware order, log keys, error codes or CLI flags change. Update **[README.md](README.md)** only if the pitch or the headline example is affected. Keep examples compilable, including the `Example` functions of `example_test.go` files, which pkg.go.dev shows and `go test` runs when they end with an `// Output:` comment.
2. If you fix an item from [Known gaps](#known-gaps), remove it from this file. If you discover a new one, add it.
3. Package-level `README.md` files under `commands/` must not contradict REFERENCE.md.
4. The Adapters section of REFERENCE.md summarizes forgo-adapter. Keep it in line with forgo-adapter's REFERENCE.md, which is the source of truth for adapter configs.

### Releasing

Versions follow semantic versioning. `go get` picks the latest tag, and pkg.go.dev documents it.

1. Check that CI passes on `main`.
2. Tag the commit and push the tag: `git tag v1.2.3 && git push origin v1.2.3`. CI runs on the tag, then creates the GitHub release with generated notes and asks the module proxy for the version, so that pkg.go.dev lists it.
3. Bump forgo-adapter to the release with `go get github.com/efegreat/forgo@v1.2.3` when it needs the release.

### Checklists

**New server**
- [ ] `Config` embeds `forgo.ServerConfig`; value receivers; `Sanitize()` with defaults; `Name()` and `New(r)`. The server type is unexported.
- [ ] All resolution happens in `New`. The chain is built once, honoring `DisableBaseMiddlewares`, then server and narrower middlewares.
- [ ] `Init` stores its context, does not block and starts no goroutines. `Serve` blocks. `Terminate` stops gracefully: it joins everything, and returns `ctx`'s error without forcing anything when `ctx` ends. `Kill` force-stops, and `App` calls it whenever `Terminate` fails or times out.
- [ ] Operation type constant, `State` type, log keys in `logger/constant.go`.
- [ ] If it logs operation input, a `SensitiveFields []string` config field masked through `redact`.
- [ ] Per-operation `Args`: clone static args once in `New` and wrap them with `types.NewMapCopyOnWrite` per operation, or use `new(types.Map[any, any])` when there are none.
- [ ] REFERENCE.md section and the middleware-order table.

**New middleware**
- [ ] Lives in `middleware/` only if it is transport-agnostic. Otherwise it goes in the server package.
- [ ] Safe for concurrent use. No per-operation state in closures.
- [ ] Nil-tolerant options, like the existing ones (`nil` adder or hook means no-op).

**New command**
- [ ] Implements `Init`/`Validate`/`Run`. A constructor returns a `cli.CommandFactory` that creates a fresh `Command` each time.
- [ ] `Validate` rejects irrelevant flags. `Run` respects `ctx`.
- [ ] REFERENCE.md "Built-in commands" entry.

## Known gaps

When you find one, verify it against the code and add it here with a `file:line` reference. Delete it in the change that fixes it.

- OpenAPI documents of the live config admin routes have no body for `SetLiveConfigHandler` and no schema for config values in `LiveConfigsHandler` results, although the catalog knows every type (`server/http/liveconfig.go:116`, `server/http/liveconfig.go:153`). Such routes are usually private and hidden with `RouteConfig.DisableAPIDoc`.
- OpenAPI documents leave out what the HTTP transport and built-in middlewares read and answer, because the generator only sees middlewares attached with `http.MiddlewareAs`: the incoming `X-Request-ID` header (`server/http/server.go:154`), the bearer token and cookie of `JWTAuth`, which need a security scheme rather than a header parameter and today have to be added with an `openapi.WithDocument` hook (`server/http/middleware.go:542`), and the 429 response and `RateLimit-*`/`Retry-After` headers of `RateLimit` (`server/http/middleware.go:429`).
- `http.JWTAuth`, `http.RateLimit` and `http.CORS` read `*http.State`, so the gRPC server has no built-in authentication or rate limiting middleware (`server/http/middleware.go:392`, `server/http/middleware.go:508`). The plan is to move key lookup, verification and `IRateLimiter` to a transport-neutral package with thin per-transport middlewares, which changes a contract forgo-adapter implements.
- On the gRPC server, grpc-go answers a request message it cannot decode with `Internal`, before forgo sees the call, although the client sent it (`server/grpc/server.go:167`). Its status has no `ErrorInfo`. Only a codec of forgo's own, decoding raw frames itself, could answer `InvalidArgument`.
- The gRPC server logs only the request of unary and server-streaming methods and the response of unary and client-streaming methods, and no response metadata (`server/grpc/logging.go:81`).
- The gRPC server receives the request of a server-streaming method before the chain, so a stream that a middleware or a stream interceptor wraps never sees it, unlike in grpc-go, where the handler receives it through the wrapper (`server/grpc/server.go:167`).
- Business structs cannot promote fields from an embedded pointer to the gRPC server's conversions; `New` rejects them (`server/grpc/binding.go:335`).
- A `transport.IConnIOObserver` wraps every connection, so grpc-go no longer sets `TCP_USER_TIMEOUT` from `Keepalive.Timeout` on Linux, which it does only on a `*net.TCPConn` (`transport/observer.go:63`).
- The HTTP server's `Terminate` does not wait for hijacked connections, such as WebSockets, because `http.Server.Shutdown` does not track them (`server/http/server.go:79`). Their handlers can still run while the resources close. `Config` does not expose `RegisterOnShutdown` to tell them to stop either.
