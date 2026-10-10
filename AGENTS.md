# AGENTS.md

Guide for coding agents and contributors working **on forgo itself**. For how to *use* forgo, see [REFERENCE.md](REFERENCE.md). [README.md](README.md) is the public entry point.

## What forgo is

A Go framework that implements the technical scaffold every backend needs (dependency wiring, server lifecycle, operations and middleware, logging, CLI, migrations, OpenAPI), so applications only write business code. Module path: `github.com/efegreat/forgo`. Go 1.27, and the code relies on **generic methods** (`func (r Resolver) Resolve[T any]()`).

## Design principles

Every change must serve these four goals. When they conflict, simplicity for the user wins, unless it costs runtime work.

1. **Simplicity.** Users declare configuration and hand over constructors. New features are exposed as config structs whose zero values work. `Sanitize()` fills defaults. No imperative registration APIs, no required boilerplate.
2. **Speed.** Everything is resolved before serving. Reflection, `Resolver` calls and handler or chain construction happen only in `Bootstrap.Build`, `ServerFactory.New` and `IServer.Init`. The request path runs pre-built closures only. The bootstrap is sealed before `Serve`, so it never becomes a runtime service locator.
3. **Abstraction.** Every unit of work is an `operation`. Transports translate their input into `*operation.Context` (`Type`, `Name`, `ID`, `Args`, `State`) and run an `operation.Handler` chain. Cross-cutting behavior is an `operation.Middleware` that works on every transport. Read transport `State` only when unavoidable.
4. **Scalability.** Everything is replaceable behind interfaces: `IServer`, `ServerFactory`, `Resource`, `ResourceFactory`, `transport.ISerializer`, binders, `operation.ILocker`, `operation.Transactor`, `worker.IBroker`, outbox/inbox storages. Core depends on interfaces only.

## Repository map

| Path | Responsibility |
| --- | --- |
| `app.go` | `App`: bootstrap → create and init servers → seal → serve → parallel termination → `process.WaitGoroutines` → close resources. |
| `bootstrap.go` | `Bootstrap`, `Resolver` (`Resolve`, `ResolveResource`, `Call`), `Resource`/`ResourceFactory`, `MiddlewareFactory`, `MiddlewareAs`, `ResolveMiddlewares`. |
| `server.go` | `IServer`, `ServerFactory`, `TermSig`, `TermConfig`, `ServerConfig` (embedded by every server config). |
| `env.go` | `Env(key, default)`. |
| `operation/` | `Context`, `Handler`, `Middleware`, `Chain`, `Error`, `Status`, `ErrorSeverity`, `ErrorMapper`, `Lock`/`ILocker`, `Tolerance`, `Transactor`. Must stay transport-agnostic and dependency-light. |
| `middleware/` | Generic middlewares: `ID`, `Logger`, `Recorder`, `Recovery`, `Locker`, `Tolerance`, `Guard`. No transport imports. |
| `server/http/` | HTTP server (std `net/http` + `ServeMux` patterns), route and handler factories, bingo binder, JSON envelope serializer, error mappers, request/response recording. `middleware.go` holds `ErrorResponder` and the opt-in `CORS`, `RateLimit`/`RateLimitWith` and `JWTAuth` middlewares, backed by `ratelimit.go` (`IRateLimiter`, in-memory GCRA limiter, `ClientIP`) and `jwt.go` (key lookup and key rules around golang-jwt v5 verification, `JWTKeys`, `JWKS`). `config.go` also holds `MiddlewareAs[Params]`, a `forgo.MiddlewareAs` whose `Params` documents the request parameters a middleware reads, and `MiddlewareContractOf`, which reads `Params` back by calling the factory against a private, empty bootstrap. |
| `server/cron/` | Cron server (robfig/cron v3), `IJob`, `cron.As`. |
| `server/worker/` | `ConsumerServer` (inbox, idempotency, attempts, transactor) and `ProducerServer` (outbox relay); broker, publisher, subscriber, delivery and storage contracts; `Message`. |
| `server/grpc/` | Only the `operation.Status` → gRPC code mapping so far. There is no gRPC server yet. |
| `cli/` | `Cli` runner, `Command`/`CommandFactory`, `cli.As`. |
| `commands/migration/` | `migrate` command: planner/executor over sql-migrate sources, MySQL/Postgres advisory lockers, file creator. |
| `commands/openapi/` | OpenAPI 3.1 generator and command, driven by `http.Config`, serializer probing and the `Params` of `http.MiddlewareAs` middlewares. |
| `logger/` | `logger.Config` (zerolog + lumberjack), `NewCtx`/`FromCtx`, and **all** log key constants. |
| `process/` | `process.Go`/`WaitGoroutines` (global tracked wait group), `Runner`, `ConcurrentRunner`, `ChannelSemaphore`. |
| `transport/` | `ISerializer`, connection observer (`IConnObserver`, `Listener`, `Conn`). |
| `types/` | Concurrency-safe `Map`, SQL-ready `JSONMap`. |
| `internal/headerlog/` | Reflection-free JSON encoding of logged headers, byte-identical to `encoding/json`. Shared by the HTTP and worker recorders. |
| `internal/redact/` | Masking of `SensitiveFields` in logs: `Fields` (a name set matched like `strings.EqualFold`), allocation-free JSON body masking (`LogJSON`), and binding errors without the input they quote. Shared by the HTTP and worker recorders and binders. |

**Adapters** (concrete resources and providers: SQL databases, Redis, RabbitMQ, outbox/inbox storages, the Redis locker and rate limiter) belong to the adapter library, [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/AGENTS.md), a separate module, `github.com/efegreat/forgo-adapter`. Core packages must never import adapters, and no adapter dependency (a driver, broker or cache client) may enter forgo's `go.mod`. Adapters import core and implement its interfaces. A resource's `Value()` must return the exact type providers are expected to request. When you change a contract that adapters implement (`worker.IBroker`, `IPublisher`, `ISubscriber`, `IDelivery`, `InboxStorage`, `OutboxStorage`, `operation.ILocker`, `http.IRateLimiter`), update forgo-adapter in the same change set and run its tests.

## Architecture invariants

Do not break these without an explicit decision from the maintainer.

### Lifecycle

- Order: `Build` (resources, then providers) → for each server `New(resolver)` then `Init(ctx)` → `Seal` → `Serve` in goroutines → on signal or first `Serve` error, terminate all servers in parallel (`TermSig.Signal` + `Wait` → `Terminate(ctx with TermTimeout)` → `Kill` on deadline) → `process.WaitGoroutines()` → `Bootstrap.Close` (reverse order).
- `Init` receives a context whose logger already carries `app_name`, `app_id`, `server_name` and `server_id`. Servers store this context and derive everything from it.
- `Serve` blocks. It returns `nil` on graceful stop, and a non-nil error stops the whole app. `Terminate` must make `Serve` return and join every goroutine the server started.

### Bootstrap

- No topological sorting. Providers are invoked in declaration order and may only depend on earlier bindings. This is deliberate: keep it.
- Matching is `AssignableTo` over all bindings: exactly 1 match → inject, 0 → `ErrMissingDependency`, 2 or more → `ErrAmbiguousDependency`. Never add "best match" heuristics.
- Providers bind under their declared result type. Resources bind under the dynamic type of `Value()`. Duplicate provider result types are rejected up front.
- `boot:"name"` tags resolve **resources only**, by `Resource.Name()`.
- `Resolver.Call` constructs entry points (handlers, middlewares, jobs, commands) and **never registers** them.
- Constructors are invoked through `invokeConstructor` (panics → errors). A nil result is an error. Failed builds roll back the resources created so far.

### Server config pattern

Every server follows the same shape. Copy it for new servers:

- A `Config` value type embeds `forgo.ServerConfig` and implements `ServerFactory` with **value receivers**: `Name()` and `New(r forgo.Resolver)`.
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
| HTTP | `ID` → `Logger` → `Recorder` → `ErrorResponder` → `Recovery(PanicRecovery)` |
| Cron | `ID` → `Logger` → `Locker` → `Recorder` → `Tolerance` → `Recovery` |
| Worker consumer | `ID` → `Logger(msg_id)` → `Locker` → `Recorder` → `Recovery(PanicRecovery)` → `Guard(idempotency)` |
| CLI | `ID` → `Logger` → `Locker` → `Recorder` → `Recovery` |

Opt-in HTTP middlewares (`CORS`, `RateLimit`, `JWTAuth`) are never part of a base chain. Their constructors take a config and return a constructor for `forgo.MiddlewareAs`, so config errors fail in `New` and dependencies (such as `IRateLimiter` for `RateLimitWith`) come from bootstrap.

Positions matter. `Recorder` sits outside `Recovery` so panics are logged as failures. `Locker` sits outside `Recorder`/`Tolerance` so the lock spans retries. HTTP error responses are written by the terminal handler wrapper. Any error that reaches `ErrorResponder` with nothing written yet, such as a middleware rejection, is written there, inside `Recorder` so the logged response is the real one. The transport repeats that check after the chain for `DisableBaseMiddlewares`.

### Operations and errors

- Transport operation types are constants: `http_handler`, `cron_job`, `message_handler`, `command`. Each transport defines its own `State` type.
- Business errors are `operation.Error`. `Message` is public; the wrapped error is private and only logged. `Status` is semantic; transports map it (`server/http/status.go`, `server/grpc/status.go`). `Severity` decides the log level in `Recorder` (Light → warn, otherwise error).
- Terminal handlers return the `operation.Error` they serialized, so `Recorder` can log the code and severity.
- `operation.Transactor.Do(ctx, fn)` calls `fn` with a context carrying the transaction. The worker copies the `*operation.Context` onto that context, so handlers always receive an `*operation.Context`.
- Retries never wait in the consumer, so they hold no concurrency slot. `IDelivery.Retry` receives the failed attempt from the inbox (0 before it was counted), and the broker adapter turns it into a delay from its own configuration, such as forgo-adapter's RabbitMQ retry queues. Without one, the broker redelivers at once.
- The consumer settles every delivery exactly once. `State.Delivery` passes only the first `Complete`, `Retry` or `Reject` to the broker. After the chain returns, the consumer settles a delivery the chain left unsettled: it acknowledges on `nil` and retries on an error. This happens outside the chain, so it also covers the middlewares outside `Recorder` and `DisableBaseMiddlewares`. An error path may return without settling and leave the delivery to the consumer.

### Logging

- Use `logger.FromCtx(ctx)`. Never create ad-hoc loggers on the request path. The returned logger is shared and must not be mutated: derive one with `With()` and store it with `logger.NewCtx`.
- Every log key lives in `logger/constant.go`. Add new keys there and never inline string keys.
- Error logs carry `Int(logger.ErrorSeverityLogKey, int(operation.SeverityX))`.
- Never log secrets. Request and message recorders mask the `SensitiveFields` of their config in headers, query, path, JSON bodies and binding errors (HTTP also always masks the credential headers), and cap logged bodies and headers (32 KiB). Masking is built before serving and is skipped entirely while `SensitiveFields` is empty.
- Log header maps with `headerlog.Log` and a transport-specific `ValueAppender` that does the masking, never with `json.Marshal`. Log JSON bodies with `redact.Fields.LogJSON`.

### Goroutines

- Work that must finish before the process exits uses `process.Go`, which `App` and `Cli` wait on before closing resources.
- Servers own their internal goroutines (`sync.WaitGroup.Go`) and join them in `Terminate`.
- `context.WithoutCancel` is used where operations must not be cut by a parent cancellation (cron jobs, message handling, HTTP `BaseContext`).

## Coding conventions

- Match the surrounding code. Doc comments on exported identifiers explain *why* and the contract, not the obvious.
- Errors: prefix with the component (`bootstrap: …`, `outbox: …`, `openapi: …`), wrap with `%w`, and declare sentinel errors as package-level `var`s. Use `errors.Join` for cleanup errors.
- Prefer the standard library. Current dependencies: zerolog, lumberjack, robfig/cron, bingo, go-playground/validator, retry-go, uuid, xid, sql-migrate, lib/pq, go-sql-driver/mysql, grpc (codes only), golang-jwt/jwt/v5 (JWT verification in `JWTAuth`; security-critical parsing is not reimplemented). Do not add a dependency without a strong reason, and never add a web framework (the HTTP server was moved off Gin to `net/http` on purpose).
- Configs are plain values that can be declared as package-level `var`s. Avoid pointers and constructors for configuration.
- Interfaces are named with an `I` prefix for framework contracts (`IServer`, `IJob`, `IBroker`, `IBinder`) or by role where that reads better (`Resource`, `Transactor`, `Runner`).
- Tests: table-driven where possible, `go-sqlmock` for SQL, `httptest` for HTTP. Keep tests passing under `-race`.

## Workflow

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
```

When you change behavior:

1. Update **[REFERENCE.md](REFERENCE.md)** in the same change whenever public API, defaults, config fields, middleware order, log keys, error codes or CLI flags change. Update **[README.md](README.md)** only if the pitch or the headline example is affected. Keep examples compilable.
2. If you fix an item from [Known gaps](#known-gaps), remove it from this file. If you discover a new one, add it.
3. Package-level `README.md` files under `commands/` must not contradict REFERENCE.md.
4. The Adapters section of REFERENCE.md summarizes forgo-adapter. Keep it in line with forgo-adapter's REFERENCE.md, which is the source of truth for adapter configs.

### Checklists

**New server**
- [ ] `Config` embeds `forgo.ServerConfig`; value receivers; `Sanitize()` with defaults; `Name()` and `New(r)`.
- [ ] All resolution happens in `New`. The chain is built once, honoring `DisableBaseMiddlewares`, then server and narrower middlewares.
- [ ] `Init` stores its context and does not block. `Serve` blocks. `Terminate` joins everything and respects its context. `Kill` force-stops.
- [ ] Operation type constant, `State` type, log keys in `logger/constant.go`.
- [ ] If it logs operation input, a `SensitiveFields []string` config field masked through `internal/redact`.
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

- OpenAPI documents leave out what the HTTP transport and built-in middlewares read and answer, because the generator only sees middlewares attached with `http.MiddlewareAs`: the incoming `X-Request-ID` header (`server/http/server.go:143`), the bearer token and cookie of `JWTAuth`, which need a security scheme rather than a header parameter (`server/http/middleware.go:551`), and the 429 response and `RateLimit-*`/`Retry-After` headers of `RateLimit` (`server/http/middleware.go:438`).
