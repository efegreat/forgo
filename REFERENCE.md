# Forgo Reference

The complete guide to building with forgo. New here? Start with the [README](README.md).

## Contents

- [Overview](#overview)
- [Quick start](#quick-start)
- [Resources and providers](#resources-and-providers)
- [Bootstrap](#bootstrap)
- [App](#app)
- [Operations](#operations)
- [Middleware](#middleware)
- [HTTP server](#http-server)
- [Cron server](#cron-server)
- [Worker](#worker)
- [Custom servers](#custom-servers)
- [CLI](#cli)
- [Built-in commands](#built-in-commands)
- [Logging](#logging)
- [Background goroutines](#background-goroutines)
- [Utilities](#utilities)
- [Adapters](#adapters)
- [Error code reference](#error-code-reference)

---

## Overview

Every backend needs the same technical scaffold: wiring dependencies, running servers, shutting down cleanly, logging with trace context, retrying, locking, migrating databases, documenting APIs. Forgo implements that scaffold. You describe your system as configuration and write the business code; forgo resolves and runs everything else.

### Building blocks

| Concept | What it is | Package |
| --- | --- | --- |
| **Resource** | A long-lived external connection or client whose lifetime matches the application: `*sql.DB`, a broker connection, a Redis client. Created by a `ResourceFactory`, closed on shutdown. | `forgo` |
| **Provider** | A constructor for an object that *uses* resources or other providers to implement behavior: repositories, services, API clients. | `forgo` |
| **Bootstrap** | The dependency resolver. Builds resources and providers, lets servers construct their handlers, then seals itself before anything runs. | `forgo` |
| **Server** | A long-running unit that serves work: HTTP, cron, worker, or your own. You declare it as a config value that implements `ServerFactory`. | `forgo/server/...` |
| **App** | Runs one or more servers as a whole and manages their lifecycle: start, graceful termination, forced termination. | `forgo` |
| **Cli** | Runs one named command per invocation, using the same resource/provider model as an App. | `forgo/cli` |
| **Operation** | The transport-agnostic unit of work. An HTTP request, a cron run, a consumed message and a CLI command are all operations. | `forgo/operation` |
| **Middleware** | `func(operation.Handler) operation.Handler`. Because everything is an operation, a middleware works on every server. | `forgo/middleware` |

> **Resource or provider?** If the object owns a long-lived external connection whose lifetime should match the application, it is a resource. If it merely uses resources or other dependencies to implement application behavior, it is a provider. `*sql.DB` is a resource; a `UserRepository` holding that `*sql.DB` is a provider.

### Lifecycle

Dependencies are dynamic while the application is being constructed and static once it starts running:

```text
forgo.NewApp(options...)          stores configuration only
app.Run()
 ├─ Bootstrap.Build()             resources (in order) → providers (in order)
 ├─ for each server:
 │    ServerFactory.New(resolver) resolve handlers, middlewares, jobs, commands
 │    IServer.Init(ctx)           ctx carries the server's logger
 ├─ Bootstrap.Seal()              dependency resolution is switched off
 ├─ IServer.Serve()               every server in its own goroutine
 │        … until SIGINT/SIGTERM, or any Serve returns an error
 ├─ terminate every server in parallel
 │    TermSig.Signal() + wait → Terminate(ctx with TermTimeout) → Kill() on timeout
 ├─ process.WaitGoroutines()      wait for tracked background goroutines
 └─ Bootstrap.Close()             resources closed in reverse creation order
```

No reflection or dependency lookup happens after `Seal`. Request handling runs pre-built handler chains only.

### Packages

| Import path | Contents |
| --- | --- |
| `forgo` | `App`, `Bootstrap`, `Resolver`, `Resource`, `ServerFactory`, `IServer`, `ServerConfig`, `MiddlewareFactory`, `Env` |
| `forgo/operation` | `Context`, `Handler`, `Middleware`, `Chain`, `Error`, `Status`, `ErrorMapper`, `Lock`, `ILocker`, `Tolerance`, `Transactor` |
| `forgo/middleware` | Built-in middlewares: `ID`, `Logger`, `Recorder`, `Recovery`, `Locker`, `Tolerance`, `Guard` |
| `forgo/server/http` | HTTP server |
| `forgo/server/cron` | Cron server |
| `forgo/server/worker` | Message consumer and outbox producer servers, broker and storage contracts |
| `forgo/cli` | Command-line runner |
| `forgo/commands/migration` | SQL migration command |
| `forgo/commands/openapi` | OpenAPI 3.1 generator command |
| `forgo/logger` | Logger configuration, context propagation, log keys |
| `forgo/process` | Tracked goroutines, runners, semaphores |
| `forgo/transport` | Serializer and connection observer contracts |
| `forgo/types` | Concurrency-safe `Map` and SQL-ready `JSONMap` |

### Requirements

- **Go 1.27 or newer.** Forgo uses generic methods, such as `r.Resolve[T]()` and `m.GetTyped[T](key)`.
- The package `forgo/server/http` is named `http`. When a file also needs the standard library, alias one of them, for example `nethttp "net/http"`.

---

## Quick start

A small user API backed by PostgreSQL.

**Domain:** a service that depends on a repository interface it owns.

```go
// user/user.go
package user

import (
	"context"

	"forgo/operation"
)

type User struct {
	ID    string
	Email string
}

// Repository is the port the domain needs. An adapter implements it.
type Repository interface {
	FindByID(ctx context.Context, id string) (User, bool, error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

var ErrNotFound = operation.Error{
	Code:     "user_not_found",
	Status:   operation.StatusNotFound,
	Message:  "user not found",
	Severity: operation.SeverityLight,
}

type GetReq struct {
	ID string `uri:"id" validate:"required"`
}

type GetResp struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func (s *Service) Get(ctx context.Context, req GetReq) (GetResp, error) {
	u, found, err := s.repo.FindByID(ctx, req.ID)
	if err != nil {
		return GetResp{}, err
	}
	if !found {
		return GetResp{}, ErrNotFound
	}
	return GetResp{ID: u.ID, Email: u.Email}, nil
}
```

**Adapter:** a repository that needs a `*sql.DB`.

```go
// postgresrepo/user.go
package postgresrepo

type UserRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) FindByID(ctx context.Context, id string) (user.User, bool, error) {
	var u user.User
	err := r.db.QueryRowContext(ctx, `SELECT id, email FROM users WHERE id = $1`, id).Scan(&u.ID, &u.Email)
	if errors.Is(err, sql.ErrNoRows) {
		return user.User{}, false, nil
	}
	if err != nil {
		return user.User{}, false, err
	}
	return u, true, nil
}
```

**Configuration:** resources and servers are plain values.

```go
// config/config.go
package config

import (
	"time"

	"forgo"
	"forgo/logger"
	"forgo/server/http"
	"yourapp/user"

	"github.com/rs/zerolog"
	// sqldb is the SQL resource from the forgo adapter library.
)

var Logger = logger.Config{
	Level:   zerolog.InfoLevel,
	Console: logger.ConsoleConfig{Enabled: true},
}

var DB = sqldb.Config{
	Name:           "main",
	DriverName:     "postgres",
	DSN:            forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/app?sslmode=disable"),
	ConnectTimeout: 5 * time.Second,
}

var HTTP = http.Config{
	Addr: forgo.Env("HTTP_ADDR", ":8080"),
	Routes: []http.RouteConfig{
		{
			Name:        "health",
			Description: "Health check endpoint",
			Method:      "GET",
			Path:        "/health",
			Version:     "v1",
			Handler:     http.EmptyHandler(),
			Args:        map[any]any{http.ArgKeyRecorderDisabled: true},
		},
		{
			Name:        "getUser",
			Description: "Get a user by id",
			Method:      "GET",
			Path:        "/users/{id}",
			Version:     "v1",
			Handler:     http.HandlerFor((*user.Service).Get), // bootstrap resolves *user.Service
		},
	},
}
```

**Entrypoint:**

```go
// main.go
package main

import (
	_ "github.com/lib/pq" // database/sql driver

	"forgo"
	"yourapp/config"
	"yourapp/postgresrepo"
	"yourapp/user"
)

func main() {
	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB),
		forgo.WithProviders(
			postgresrepo.NewUserRepository, // needs *sql.DB → the "main" resource
			user.NewService,                // needs user.Repository → *postgresrepo.UserRepository
		),
		forgo.WithServers(config.HTTP),
	)
	app.Run()
}
```

```console
$ curl localhost:8080/v1/users/42
{"reqId":"0199…","status":200,"result":{"id":"42","email":"ada@example.com"}}

$ curl localhost:8080/v1/users/404
{"reqId":"0199…","status":404,"errCode":"user_not_found","errMsg":"user not found"}
```

The app runs until it receives SIGINT or SIGTERM, then shuts down as configured.

---

## Resources and providers

### Resources

```go
type Resource interface {
	Name() string // unique, non-empty
	Value() any   // the dependency providers receive, e.g. *sql.DB
	Close() error // called on shutdown, in reverse creation order
}

type ResourceFactory interface {
	New() (Resource, error)
}
```

A resource factory is usually a config struct. `New` must fully initialize the resource and verify it, for example by pinging the database. If `New` returns without error, the resource is expected to be ready.

The **dynamic type of `Value()`** is the type that providers ask for. A SQL resource returning a `*sql.DB` satisfies every provider parameter of type `*sql.DB`. A broker resource returning `*rabbitmq.Broker` satisfies parameters of type `worker.IBroker`.

Writing your own resource:

```go
package redisres

import (
	"context"
	"fmt"
	"time"

	"forgo"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Name string
	Addr string
}

func (c Config) New() (forgo.Resource, error) {
	cl := redis.NewClient(&redis.Options{Addr: c.Addr})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.Ping(ctx).Err(); err != nil {
		_ = cl.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}

	return &Resource{name: c.Name, cl: cl}, nil
}

type Resource struct {
	name string
	cl   *redis.Client
}

func (r *Resource) Name() string { return r.name }
func (r *Resource) Value() any   { return r.cl } // providers ask for *redis.Client
func (r *Resource) Close() error { return r.cl.Close() }
```

```go
forgo.WithResources(redisres.Config{Name: "cache", Addr: "localhost:6379"})
```

### Providers

A provider is a constructor function. Its parameters are its dependencies and its first result is what it provides:

```go
func(deps...) T
func(deps...) (T, error)
```

- It must be a non-nil, non-variadic function.
- The second result, if present, must be `error`.
- `T` cannot be `error`.
- It must not return a nil value. A nil pointer, map, slice, func, channel or interface fails the build.
- No two providers may have the same result type `T`.

```go
func NewUserRepository(db *sql.DB) *UserRepository
func NewPaymentClient(cfg PaymentConfig) (*PaymentClient, error)
func NewOrderService(repo order.Repository, pay *PaymentClient) *order.Service
```

Each provider is called **exactly once**, during `Build`, whether or not anything uses it. Provided values are application-wide singletons.

---

## Bootstrap

`Bootstrap` is forgo's dependency resolver. Apps and the CLI drive it for you, but you can also use it directly, for example in tests.

### Phases

1. **Validate.** Provider signatures are checked before any resource is opened.
2. **Resources.** Resource factories are called in declaration order.
3. **Providers.** Providers are called in declaration order. Each argument is resolved from the resources and the providers declared before it.
4. **Construction.** Server factories (`ServerFactory.New`) receive a `Resolver` and construct handlers, middlewares, jobs and commands from it.
5. **Seal.** After every server is created and initialized, resolution is switched off. Any later `Resolver` call returns `bootstrap: resolver is sealed`.
6. **Close.** On shutdown, resources are closed in reverse creation order.

If any step of `Build` fails, the resources created so far are closed in reverse order before the error is returned.

### Resolution rules

1. **Declaration order is the dependency order.** A provider can only depend on resources and on providers declared **before** it. Bootstrap does not reorder providers or sort them topologically. Declare providers from innermost to outermost:

   ```go
   forgo.WithProviders(
   	postgresrepo.NewUserRepository, // depends on *sql.DB (a resource)
   	user.NewService,                // depends on user.Repository
   	billing.NewService,             // depends on *user.Service
   )
   ```

   Declaring them the other way around fails at startup with a precise message:

   ```text
   bootstrap: provider yourapp/user.NewService[0]: bootstrap: constructor yourapp/user.NewService
   argument 0 (user.Repository): missing dependency user.Repository
   ```

2. **Matching is by assignability.** For every parameter type, bootstrap collects every resource value and provider result *assignable* to it.
   - **Exactly one** match is injected.
   - **None** fails with `ErrMissingDependency`.
   - **Two or more** fail with `ErrAmbiguousDependency`, listing every candidate.

   There is no "closest match" preference. If two providers both satisfy an interface, any request for that interface is ambiguous.

3. **Binding type.** A provider binds under its *declared* result type. A resource binds under the *dynamic* type of its `Value()`.
   - A provider returning `*UserRepository` (concrete) satisfies requests for `*UserRepository` and for every interface it implements, such as `user.Repository`.
   - A provider returning `user.Repository` (interface) satisfies requests for `user.Repository`. It also satisfies requests for any interface that `user.Repository` itself implements. The concrete type stays hidden.

   The recommended style: domain services depend on interfaces they own, adapters return concrete types, and bootstrap connects them.

4. **Named resources.** When two resources share a type, such as two `*sql.DB`, a plain `*sql.DB` parameter is ambiguous. Select resources by name with a *dependency struct*: a struct, or pointer to struct, parameter with at least one `boot` tag.

   ```go
   type ReportRepositoryDeps struct {
   	Primary *sql.DB `boot:"primary"` // Resource.Name() == "primary"
   	Replica *sql.DB `boot:"replica"`
   	Clock   Clock                    // untagged exported field: resolved by type
   	cache   *Cache                   // unexported, untagged: ignored, left zero
   }

   func NewReportRepository(d ReportRepositoryDeps) *ReportRepository
   ```

   | Field | Behavior |
   | --- | --- |
   | `boot:"name"` | Resolves the resource whose `Name()` is `name`. Its value must be assignable to the field type. Only **resources** are named; providers are not. |
   | `boot:"-"` | Skipped, left zero. |
   | Untagged, exported | Resolved by type, like a normal parameter. |
   | Untagged, unexported | Ignored. |
   | Tagged, unexported, or `boot:""` | Build error. |

5. **Failures are errors, not crashes.** A provider that returns an error, returns nil, or panics fails the build. Panics are recovered and reported with a stack trace.

6. **Entry points are not providers.** HTTP handlers, middlewares, cron jobs, message handlers and commands are constructed by server factories through `Resolver.Call`. Their results are **not** registered, so nothing can depend on a handler.

### Designing the graph from top to bottom

Think of the graph in layers. Each layer only depends on the layers above it:

```text
Resources        *sql.DB, worker.IBroker, *redis.Client            forgo.WithResources
     ↓
Adapters         repositories, outbox/inbox storage, lockers,      forgo.WithProviders (first)
                 transactor, external API clients
     ↓
Domain services  user.Service, order.Service, billing.Service      forgo.WithProviders (after adapters)
     ↓
Entry points     HTTP handlers, middlewares, cron jobs,            server configs: http.HandlerFor/HandlerAs,
                 message handlers, commands                        forgo.MiddlewareAs, cron.As, cli.As, worker.HandlerFor/HandlerAs
```

Guidelines:

- List providers in this order and the declaration order takes care of itself.
- Keep provider constructors cheap and side-effect free. Open connections in resources, not providers.
- Prefer one provider per result type. When two implementations of the same interface must coexist, give consumers distinct, narrower interfaces, or have one provider return a type the other does not satisfy.
- Every `WithProviders` call appends, so you can compose provider groups and keep their order: `forgo.WithProviders(config.Adapters...)`, then `forgo.WithProviders(config.Services...)`.

### Resolver

`ServerFactory.New` receives a `forgo.Resolver`. It is valid from `Build` until `Seal`.

| Method | Use |
| --- | --- |
| `r.Resolve[T]() (T, error)` | Returns the single resource or provider value assignable to `T`. |
| `r.ResolveResource[T](name) (T, error)` | Returns the resource named `name` as `T`. |
| `r.Call[T](constructor) (T, error)` | Calls `constructor`, resolving every argument with the provider rules, including dependency structs. Its result must be assignable to `T` and is **not** registered. |

```go
svc, err := r.Resolve[*user.Service]()
db, err := r.ResolveResource[*sql.DB]("replica")
mw, err := r.Call[operation.Middleware](NewAuthMiddleware)
```

Sentinel errors: `forgo.ErrMissingDependency`, `forgo.ErrAmbiguousDependency` and `forgo.ErrResourceNotFound`. Check them with `errors.Is`. Built-in servers use `ErrMissingDependency` to treat optional dependencies, such as `operation.ILocker`, as absent.

### Using Bootstrap directly

```go
boot := forgo.NewBootstrap(
	[]forgo.ResourceFactory{config.DB},
	[]any{postgresrepo.NewUserRepository, user.NewService},
)
r, err := boot.Build()
if err != nil {
	return err
}
defer boot.Close()

svc, err := r.Resolve[*user.Service]()
// ...
_ = boot.Seal()
```

This is handy in tests. Swap a provider for a fake and let bootstrap wire the rest:

```go
boot := forgo.NewBootstrap(nil, []any{
	func() user.Repository { return fakeRepo{} },
	user.NewService,
})
```

---

## App

```go
app := forgo.NewApp(
	forgo.WithName("shop"),                    // default "default"
	forgo.WithLogger(config.Logger),           // logger.Config
	forgo.WithResources(config.DB, ...),       // forgo.ResourceFactory values
	forgo.WithProviders(NewRepo, NewSvc, ...), // constructors, innermost first
	forgo.WithServers(config.HTTP, ...),       // forgo.ServerFactory values
)
app.Run()
```

`Run` blocks until the application stops. It panics if bootstrap fails, or if a server's `New` or `Init` fails. Resources created so far are still closed. Use `forgo.Env(key, default)` to read configuration from the environment.

### What `Run` does

1. Builds the bootstrap: resources, then providers.
2. Creates the app logger with `app_name` and `app_id` fields.
3. For each server factory, in order: calls `New(resolver)`, derives a server logger with `server_name` and `server_id`, and calls `Init(ctx)` with that logger in `ctx`.
4. Seals the bootstrap.
5. Starts every server's `Serve()` in its own goroutine.
6. Waits for SIGINT, SIGTERM, or the first `Serve` error.
7. Terminates all servers **in parallel**. For each one:
   1. If `Term().TermSig` is set, it is signaled and the app sleeps for `TermSig.Wait`. This is the window for a readiness probe to fail and load balancers to drain.
   2. Calls `Terminate(ctx)`. `ctx` expires after `Term().TermTimeout`, which defaults to 30s.
   3. If the deadline passes before `Terminate` returns, calls `Kill()`.
8. Waits for every goroutine started with [`process.Go`](#background-goroutines).
9. Closes resources in reverse creation order.

### Termination settings

Every built-in server config embeds `forgo.ServerConfig`:

```go
type ServerConfig struct {
	Term                   TermConfig
	Middlewares            []MiddlewareFactory // server-wide middlewares
	DisableBaseMiddlewares bool                // drop the server's built-in chain
}

type TermConfig struct {
	TermSig     *TermSig      // optional readiness signal
	TermTimeout time.Duration // graceful termination budget; 0 → 30s
}

type TermSig struct {
	Wait time.Duration // pause after signaling, before Terminate
}
```

**Readiness pattern.** Share one `TermSig` between the server config and a readiness route:

```go
var termSig = &forgo.TermSig{Wait: 10 * time.Second}

func NewReadiness() http.Handler[struct{}, struct{}] {
	return func(ctx context.Context, _ struct{}) (struct{}, error) {
		if termSig.Signaled() {
			return struct{}{}, operation.Error{
				Code: "terminating", Status: operation.StatusUnavailable, Message: "shutting down",
			}
		}
		return struct{}{}, nil
	}
}

var HTTP = http.Config{
	ServerConfig: forgo.ServerConfig{
		Term: forgo.TermConfig{TermSig: termSig, TermTimeout: 20 * time.Second},
	},
	Routes: []http.RouteConfig{
		{Name: "ready", Method: "GET", Path: "/ready", Version: "v1",
			Handler: http.HandlerAs[struct{}, struct{}](NewReadiness)},
	},
}
```

On SIGTERM, `/v1/ready` starts answering 503. The app waits 10s for traffic to drain, then shuts the HTTP server down gracefully within 20s.

### Running several servers in one app

Pass several server factories to the same app. They share **one** bootstrap: the same resources and the same provider instances.

```go
forgo.NewApp(
	forgo.WithName("shop"),
	forgo.WithLogger(config.Logger),
	forgo.WithResources(config.DB, config.Broker),
	forgo.WithProviders(config.Providers...),
	forgo.WithServers(
		config.HTTP,      // public API on :8080
		config.AdminHTTP, // another http.Config on :9090
		config.Cron,      // scheduled jobs
		config.Consumer,  // message handlers
		config.Producer,  // outbox relay
	),
).Run()
```

All servers start together. If any `Serve` fails, every server is terminated. On a signal, all of them terminate in parallel.

### Deploying servers separately

Keep resources, providers and server configs in a shared package and give each deployable its own `main`:

```text
yourapp/
├── config/                 resources, provider lists, server configs
├── user/ order/ billing/   domain
├── postgresrepo/           adapters
└── cmd/
    ├── api/main.go         WithServers(config.HTTP)
    ├── worker/main.go      WithServers(config.Consumer, config.Producer)
    ├── scheduler/main.go   WithServers(config.Cron)
    └── tool/main.go        cli.New(...)
```

```go
// cmd/worker/main.go
func main() {
	forgo.NewApp(
		forgo.WithName("shop-worker"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB, config.Broker),
		forgo.WithProviders(config.Adapters...),
		forgo.WithProviders(config.Services...),
		forgo.WithServers(config.Consumer, config.Producer),
	).Run()
}

// cmd/scheduler/main.go
func main() {
	forgo.NewApp(
		forgo.WithName("shop-scheduler"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB),
		forgo.WithProviders(config.Adapters...),
		forgo.WithProviders(config.Services...),
		forgo.WithServers(config.Cron),
	).Run()
}
```

Providers are eager. Give each deployable only the provider groups it needs, so the scheduler does not construct messaging adapters it never uses.

### Choosing servers at runtime

One binary, with the server set picked by environment:

```go
var servers = map[string]forgo.ServerFactory{
	"http":     config.HTTP,
	"cron":     config.Cron,
	"consumer": config.Consumer,
	"producer": config.Producer,
}

func main() {
	var selected []forgo.ServerFactory
	for _, name := range strings.Split(forgo.Env("SERVERS", "http"), ",") {
		sf, ok := servers[strings.TrimSpace(name)]
		if !ok {
			log.Fatalf("unknown server %q", name)
		}
		selected = append(selected, sf)
	}

	forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB, config.Broker),
		forgo.WithProviders(config.Providers...),
		forgo.WithServers(selected...),
	).Run()
}
```

### App and CLI in one binary

The CLI reads its command name from `os.Args[1]`, so one binary can serve and run commands:

```go
func main() {
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		if err := newCli().Run(); err != nil {
			os.Exit(1)
		}
		return
	}
	newApp().Run()
}
```

`./shop serve` runs the servers. `./shop migrate -a=up` runs a command.

---

## Operations

Forgo abstracts every unit of work as an **operation**: an HTTP request, a cron run, a consumed message, a CLI command, or anything a custom server does. Each transport translates its input into an `operation.Context` and runs a chain of `operation.Handler`s. The common execution logic is interpreted once, so the same middleware works everywhere.

### `operation.Context`

```go
type Context struct {
	context.Context
	ID    string               // operation id (HTTP: X-Request-ID or generated UUIDv7)
	Name  string               // route name, job name, message name, command name
	Type  string               // "http_handler", "cron_job", "message_handler", "command", ...
	Args  *types.Map[any, any] // per-operation values, concurrency-safe
	State any                  // transport-specific state
}
```

- It is a `context.Context`. Handlers and services receive it as their `ctx`.
- `Value(key)` checks `Args` first, then the embedded context. Static values configured on a route or message handler (`Args: map[any]any{...}`) are copied into `Args` for each operation, so `ctx.Value(myKey)` returns them anywhere downstream.
- Use your own unexported key types for `Args`. `operation.ArgKey{}` is the framework's own key type, and all of its values are equal to each other.

| Transport | `Type` | `State` |
| --- | --- | --- |
| HTTP | `http.OperationTypeHTTPHandler` (`"http_handler"`) | `*http.State{Request *http.Request, Writer}` |
| Cron | `cron.OperationTypeCronJob` (`"cron_job"`) | `nil` |
| Worker consumer | `worker.OperationTypeMessageHandler` (`"message_handler"`) | `*worker.State{Delivery worker.IDelivery, Attempts int}` |
| CLI | `cli.OperationTypeCommand` (`"command"`) | `*cli.State{Fields []string}` (raw `os.Args`) |

Inside a handler you can reach the operation directly. This only works on the context the transport passed in, not on one derived from it:

```go
func (s *Service) Get(ctx context.Context, req GetReq) (GetResp, error) {
	op := ctx.(*operation.Context)
	ip := op.State.(*http.State).Request.RemoteAddr
	// ...
}
```

### Handlers, middlewares, chains

```go
type Handler func(*Context) error
type Middleware func(Handler) Handler

func Chain(handler Handler, middlewares ...Middleware) Handler
```

`Chain(h, a, b, c)` executes `a → b → c → h`. The first middleware is the outermost. A middleware and anything it captures must be safe for concurrent use. Keep per-operation mutable state in the `operation.Context`, in `Args`, or in the transport state.

### Errors

`operation.Error` is the error type every transport understands:

```go
type Error struct {
	Code     string        // stable, machine-readable identifier
	Status   Status        // transport-agnostic classification
	Message  string        // public message, safe for clients
	Severity ErrorSeverity // drives log level and alerting
	// unexported cause: the private, internal error
}
```

| Method | Behavior |
| --- | --- |
| `Error() string` | Returns `Message`. |
| `Wrap(err) Error` | Returns a copy with `err` as the internal cause. The cause is logged, never sent to clients. |
| `Unwrap() error` | Returns the cause. |
| `Format(args...) Error` | Returns a copy with `Message` formatted by `fmt.Sprintf`. |

Declare errors once and derive from them:

```go
var ErrOrderNotFound = operation.Error{
	Code:     "order_not_found",
	Status:   operation.StatusNotFound,
	Message:  "order %s not found",
	Severity: operation.SeverityLight,
}

return ErrOrderNotFound.Format(id).Wrap(err)
```

`Format` and `Wrap` return modified copies, so match errors by code instead of with `errors.Is`:

```go
var oe operation.Error
if errors.As(err, &oe) && oe.Code == ErrOrderNotFound.Code { ... }
```

**Statuses** classify an error semantically. Each transport maps them to its own protocol codes:

| Status | Meaning | HTTP |
| --- | --- | --- |
| `StatusUnknown` | Nothing better is known | 500 |
| `StatusInvalidArgument` | Client sent invalid input | 400 |
| `StatusUnauthenticated` | Missing or invalid authentication | 401 |
| `StatusPermissionDenied` | Authenticated but not allowed | 403 |
| `StatusNotFound` | Resource does not exist | 404 |
| `StatusConflict` | Conflicts with existing state (duplicate email…) | 409 |
| `StatusAborted` | Concurrent modification or aborted transaction; retry after refresh | 409 |
| `StatusPreconditionFailed` | Business precondition not met (already paid…) | 412 |
| `StatusRateLimited` | Limit exceeded | 429 |
| `StatusCancelled` | Operation cancelled | 408 |
| `StatusTimeout` | Operation timed out | 408 |
| `StatusUnavailable` | Dependency temporarily unavailable | 503 |
| `StatusInternal` | Unexpected server error | 500 |

**Severities:** `SeverityLight` is a minor incident, logged as a warning. `SeverityModerate` is significant and `SeverityCritical` needs handling as soon as possible; both are logged as errors. Each log entry carries a numeric `severity` field (0, 1, 2) for alert routing.

**Error mapping.** An `ErrorMapper` turns arbitrary errors into `operation.Error`:

```go
mapper := operation.NewErrorMapper(
	func(err error) (operation.Error, bool) {
		if errors.Is(err, sql.ErrNoRows) {
			return operation.Error{
				Code: "not_found", Status: operation.StatusNotFound, Message: "not found",
				Severity: operation.SeverityLight,
			}.Wrap(err), true
		}
		return operation.Error{}, false
	},
)
oe := mapper.Map(err)
```

`Map` returns the error itself if it already is, or wraps, an `operation.Error`. Otherwise it tries each mapper in order. If none matches, it falls back to `operation.NewFallbackError(err)`: code `"x"`, `StatusUnknown`, message `"an error occurred"`, `SeverityCritical`.

### Locks

```go
type Lock struct {
	Key string
	Ttl time.Duration
}

type ILocker interface {
	Lock(key string, ttl time.Duration) (bool, error)
	WaitLock(ctx context.Context, key string, attempts int, delay, ttl time.Duration) (bool, error)
	Unlock(key string) error
}

type LockCrafter func(ctx *Context) Lock
```

Cron jobs, CLI commands and message handlers take a `Lock` in their config. A lock applies only when **both** `Key` and `Ttl` are set. Locking needs an `operation.ILocker` in the bootstrap, for example the Redis locker from the adapter library. Servers resolve it automatically when present. A `Lock` configured without a registered `ILocker` fails at startup.

### Tolerance (retries)

```go
type Tolerance struct {
	Retry         bool
	RetryAttempts uint          // 0 = retry until success (or RetryTimeout)
	RetryDelay    time.Duration // fixed delay between attempts
	RetryTimeout  time.Duration // overall budget; 0 = none
	RetryIf       func(error) bool                       // nil = retry every error
	OnRetry       func(ctx context.Context, attempt uint, err error)
	OnSuccess     func(context.Context)
	OnFailure     func(ctx context.Context, err error)
}

var DefaultTolerance = Tolerance{Retry: true, RetryAttempts: 5, RetryDelay: 5 * time.Second}
```

Used by cron jobs through `IJob.Tolerance()`, and by `middleware.Tolerance` anywhere else.

### Transactor

```go
type Transactor interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`Do` must run `fn` inside a transaction: commit when `fn` returns nil, roll back otherwise. It must call `fn` with a context derived from `ctx` that carries the transaction, so every repository or storage called with that context joins it. The worker consumer runs each message handler through it. The handler still receives an `*operation.Context`, now running on the transaction's context.

---

## Middleware

### Using middlewares

Middlewares are configured as `forgo.MiddlewareFactory` values, so their dependencies are resolved by bootstrap:

```go
type MiddlewareFactory func(forgo.Resolver) (operation.Middleware, error)

func MiddlewareAs(constructor any) MiddlewareFactory
```

```go
func NewAuditMiddleware(audit *audit.Log) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			err := next(ctx)
			audit.Record(ctx, ctx.Type, ctx.Name, err)
			return err
		}
	}
}

forgo.MiddlewareAs(NewAuditMiddleware) // *audit.Log is resolved by bootstrap
```

Built-in middlewares without dependencies fit in a zero-argument constructor:

```go
forgo.MiddlewareAs(func() operation.Middleware {
	return middleware.Tolerance(operation.Tolerance{Retry: true, RetryAttempts: 3, RetryDelay: time.Second})
})
```

Because every server runs operations, the **same middleware** can be attached to HTTP routes, cron servers, message handlers and CLI commands:

```go
metrics := forgo.MiddlewareAs(NewMetricsMiddleware)

http.Config{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{metrics}}, ...}
cron.Config{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{metrics}}, ...}
worker.ConsumerConfig{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{metrics}}, ...}
cli.Config{Middlewares: []forgo.MiddlewareFactory{metrics}, ...}
```

When a middleware needs transport details, switch on `State`:

```go
switch st := ctx.State.(type) {
case *http.State:
	tenant = st.Request.Header.Get("X-Tenant")
case *worker.State:
	tenant, _ = st.Delivery.Message().Headers.Get("x-tenant")
}
```

### Order

Every server builds the chain as **base middlewares → server middlewares → narrower scopes → handler**. Base middlewares are skipped when `DisableBaseMiddlewares` is true.

| Server | Base chain (outer → inner) | Then |
| --- | --- | --- |
| HTTP | `ID` → `Logger` → `Recorder` (request/response fields) → `ErrorResponder` (writes unwritten errors) → `Recovery` (serialized error) | server → route → handler |
| Cron | `ID` → `Logger` → `Locker` (job lock) → `Recorder` → `Tolerance` (job tolerance) → `Recovery` | server → job |
| Worker consumer | `ID` → `Logger` (+`msg_id`) → `Locker` (per message) → `Recorder` (headers, payload) → `Recovery` (retry/reject) → `Guard` (inbox idempotency) | server → subscriber → handler → bind/transaction/handler |
| CLI | `ID` → `Logger` → `Locker` (command lock) → `Recorder` → `Recovery` | cli → command → parse/validate/run |

### Built-in middlewares (`forgo/middleware`)

| Middleware | Behavior |
| --- | --- |
| `ID()` | Sets `ctx.ID` to a UUIDv7 if empty. |
| `Logger(adder logger.FieldAdder)` | Derives a logger with `op_type`, `op_name`, `op_id` plus fields from `adder`, and stores it in the context. |
| `Recorder(disabled, before, after)` | Logs `operation started` and then `operation completed` (with `elapsed`) or `operation failed` (with `code`, `severity`, `elapsed`, the internal cause, and a warn/error level by severity). `before`/`after` add fields; `disabled(ctx)` skips logging. |
| `Recovery(on func(*operation.Context, any) error)` | Converts panics into errors. With `on == nil`, the error contains the panic value and stack. |
| `Locker(l, crafter, onConflict)` | Acquires the lock built by `crafter` before running and releases it after. If already held, logs and calls `onConflict` (or returns nil). Skips locking when the lock's key or TTL is empty. |
| `Tolerance(conf operation.Tolerance)` | Retries the rest of the chain according to `conf`. |
| `Guard(check, reject)` | Runs `check`. If it allows, the operation proceeds; otherwise `reject` decides the outcome. Useful for authorization, feature gates and idempotency. |

---

## HTTP server

Built on the standard library `net/http` with Go's pattern-based `ServeMux`.

### Configuration

```go
var HTTP = http.Config{
	ServerConfig: forgo.ServerConfig{
		Term:        forgo.TermConfig{TermTimeout: 20 * time.Second},
		Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewMetricsMiddleware)},
	},
	Addr:       ":8080",
	Validators: map[string]validator.Func{"slug": isSlug},
	Routes:     routes,
}
```

| Field | Default | Description |
| --- | --- | --- |
| `ServerConfig` | | Termination, server-wide middlewares, `DisableBaseMiddlewares`. |
| `Network` | `"tcp"` | Listener network. |
| `Addr` | `":8080"` | Listen address. |
| `ReadTimeout` | `60s` | `http.Server.ReadTimeout`. |
| `ReadHeaderTimeout` | `0` | `http.Server.ReadHeaderTimeout`. |
| `WriteTimeout` | `10s` | `http.Server.WriteTimeout`. |
| `IdleTimeout` | `60s` | `http.Server.IdleTimeout`. |
| `MaxHeaderBytes` | `0` | `http.Server.MaxHeaderBytes`. |
| `TlsConfig` | | Serves HTTPS when set. Certificates come from `TlsConfig.Certificates`, `GetCertificate` or `GetConfigForClient`. |
| `DisableGeneralOptionsHandler`, `TlsNextProto`, `ConnState`, `ConnContext`, `Http2`, `Protocols` | | Passed through to `http.Server`. |
| `ErrorLog` | zerolog adapter | `net/http`'s internal error log, written as `http server error` entries. |
| `Observer` | `transport.LoggingObserver` | Connection observer. It logs accept errors. |
| `Validators` | | Custom validation tags for the default binder. |
| `Binder` | bingo binder | Request binder (`http.IBinder`). |
| `Serializer` | JSON envelope | Response serializer factory (`http.SerializerFactory`). |
| `ErrorMapper` | binding/validation mappers | Maps handler and binding errors to `operation.Error`. |
| `Routes` | | Route list. |

### Routes

```go
type RouteConfig struct {
	Name        string                    // operation name; OpenAPI operationId
	Description string                    // OpenAPI description
	Method      string                    // "GET", "POST", …; empty matches every method
	Path        string                    // ServeMux path pattern, e.g. "/users/{id}"
	Version     string                    // path prefix, e.g. "v1"
	Middlewares []forgo.MiddlewareFactory // route middlewares
	Handler     http.HandlerFactory       // HandlerFor / HandlerAs / EmptyHandler
	Args        map[any]any               // static values copied into ctx.Args per request
}
```

A route is registered as `"<Method> /<Version><Path>"`. Method `GET`, version `v1` and path `/users/{id}` become `GET /v1/users/{id}`. With an empty `Version`, the route is served at `<Path>` alone. `RouteConfig.FullPath()` returns the resulting path. Paths use standard `ServeMux` syntax: `{name}` wildcards, `{name...}` for the remainder, `{$}` for an exact match.

A request that no route matches, including a known path with a different method, gets a JSON `404` with `errCode: "nf"`. It runs as the `not_found` operation through the base and server middlewares. A route with an empty `Method` and the path `/` replaces this fallback.

### Handlers

All handlers share one signature:

```go
type Handler[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)
```

Three ways to declare one:

```go
// 1. A method of a provided service. Bootstrap resolves the receiver (*user.Service).
//    Req and Resp are inferred from the method.
Handler: http.HandlerFor((*user.Service).Get),

// The receiver can be an interface too: bootstrap resolves user.Reader.
Handler: http.HandlerFor(user.Reader.Get),

// 2. A handler constructor. Its parameters are resolved by bootstrap,
//    and it returns the handler func.
Handler: http.HandlerAs[user.CreateReq, user.CreateResp](user.NewCreateHandler),

func NewCreateHandler(svc *user.Service, mailer *mail.Client) http.Handler[user.CreateReq, user.CreateResp] {
	return func(ctx context.Context, req user.CreateReq) (user.CreateResp, error) {
		// ...
	}
}

// 3. No request, empty success response. Ideal for health checks.
Handler: http.EmptyHandler(),
```

Each request goes through: bind and validate `Req` → call the handler → serialize `Resp` or the mapped error.

### Request binding and validation

The default binder is [bingo](https://github.com/efegreat/bingo) with go-playground `validator`. One request struct describes every source:

```go
type UpdateProductReq struct {
	ProductID uint64  `uri:"product_id" validate:"required"`        // path wildcard {product_id}
	Name      string  `json:"name" validate:"required,min=2"`       // JSON body
	Price     float64 `json:"price" validate:"gt=0"`                // JSON body
	Currency  string  `query:"currency" validate:"required,len=3"`  // ?currency=EUR
	RequestID string  `header:"X-Request-ID"`                       // header
}
```

| Tag | Source |
| --- | --- |
| `json:"name"` | JSON request body. Numbers keep precision; unknown fields are allowed. |
| `query:"name"` | URL query parameter. |
| `uri:"name"` | Path wildcard (`r.PathValue`). |
| `header:"Name"` | Request header. |
| `validate:"..."` | go-playground validator rules, run after binding. |

A field without a source tag is not bound. Validation error messages use the tag name (`field: name, rule: min=2`). Register custom validation tags with `Config.Validators`, or replace the binder entirely with any `http.IBinder`:

```go
type IBinder interface {
	Bind(*http.Request, any) error
}
```

### Responses

The default serializer writes a JSON envelope and sets `Content-Type: application/json; charset=utf-8` and `X-Request-ID`:

```json
{"reqId": "0199c3…", "status": 200, "result": {"id": "42", "email": "ada@example.com"}}
{"reqId": "0199c3…", "status": 409, "errCode": "name_taken", "errMsg": "name already taken"}
```

- `reqId` is the operation ID: the incoming `X-Request-ID` header, or a generated UUIDv7.
- `EmptyHandler` responds without a `result` field.
- Handler errors go through the `ErrorMapper`. The `operation.Error` status picks the HTTP status code, and `Code`/`Message` become `errCode`/`errMsg`. The internal cause is only logged.
- An error that maps to nothing becomes `500` with `errCode: "x"`, `errMsg: "an error occurred"`.
- A panic in the handler or any non-base middleware is recovered and answered the same way.

Default error mappers, all exported so you can reuse them:

| Mapper | Matches | `errCode` | Status | `errMsg` |
| --- | --- | --- | --- | --- |
| `http.MapEOF` | `io.EOF`, `io.ErrUnexpectedEOF` | `eof` | 400 | `request body EOF` |
| `http.MapJSONSyntax` | `*json.SyntaxError` | `json_syntax` | 400 | `invalid json syntax` |
| `http.MapJSONType` | `*json.UnmarshalTypeError` | `json_type` | 400 | `type error, field: …, required type: …` |
| `http.MapBinding` | `bingo.FieldError` | `binding` | 400 | `invalid param for <source>: <name>` |
| `http.MapValidation` | `validator.ValidationErrors` | `validation` | 400 | `field: …, rule: …; …` |

Setting `ErrorMapper` **replaces** the defaults. Include them if you still want them:

```go
ErrorMapper: operation.NewErrorMapper(
	http.MapEOF, http.MapJSONSyntax, http.MapJSONType, http.MapBinding, http.MapValidation,
	mapSQLErrors,
),
```

**Custom serializer.** Implement `transport.ISerializer` and provide a factory:

```go
type SerializerFactory func(id string, w http.ResponseWriter) transport.ISerializer

type ISerializer interface {
	Success(result any) error // write the success response; return only write/serialization failures
	Error(err error) error    // write the error response
}
```

Follow the default serializer's convention: `Error` returns the `operation.Error` it wrote, wrapped with any write failure. This lets the `Recorder` log the error's code and severity. A serializer instance serves one request; write only once.

### Middlewares that reject requests

A middleware rejects a request by returning an error instead of calling `next`. The `ErrorResponder` base middleware maps the error and writes the response, so return an `operation.Error` to choose the status:

```go
func NewAuth(verifier *auth.Verifier) operation.Middleware {
	return middleware.Guard(
		func(ctx *operation.Context) (bool, error) {
			st := ctx.State.(*http.State)
			return verifier.Valid(st.Request.Header.Get("Authorization")), nil
		},
		func(*operation.Context) error {
			return operation.Error{
				Code: "unauthenticated", Status: operation.StatusUnauthenticated, Message: "unauthenticated",
			}
		},
	)
}
```

The error is written only if nothing has been written yet. With `DisableBaseMiddlewares`, the transport writes it after the chain returns. To add `ErrorResponder` to your own chain, use `http.ErrorResponder(serializer, errorMapper)`.

### Request logging

The base `Recorder` logs every request:

- **On start:** `addr`, `path`, `query`, `method`, `req_headers` and `req_body`, each capped at 32 KiB. Longer values carry `headers_cut`/`body_cut`.
- **On finish:** `status`, `resp_headers`, `resp_body` and `elapsed`.

Values of the headers in `http.SensitiveHeaders` are masked as `[*]`. The defaults are `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie` and `X-Api-Key`. Add your own at startup:

```go
func init() { http.SensitiveHeaders["X-Session-Token"] = struct{}{} }
```

Disable recording for noisy routes such as health checks:

```go
Args: map[any]any{http.ArgKeyRecorderDisabled: true},
```

Logging limits are exported constants: `RequestRecorderHeaderLimit`, `RequestRecorderBodyLimit`, `ResponseRecorderHeaderLimit`, `ResponseRecorderBodyLimit`.

### Shutdown

`Terminate` calls `http.Server.Shutdown`. The server stops accepting connections and waits for in-flight requests until `TermTimeout`. Combine it with a `TermSig` for [readiness-aware shutdown](#termination-settings).

---

## Cron server

Runs jobs on cron schedules, using [robfig/cron v3](https://github.com/robfig/cron).

```go
var Cron = cron.Config{
	ServerConfig: forgo.ServerConfig{
		Term: forgo.TermConfig{TermTimeout: 5 * time.Minute}, // let running jobs finish
	},
	Jobs: []cron.JobConfig{
		{Name: "expireCarts", Schedule: "*/5 * * * *", Job: cron.As(cart.NewExpireJob)},
		{
			Name:     "nightlyReport",
			Schedule: "CRON_TZ=Europe/Istanbul 0 3 * * *",
			Lock:     operation.Lock{Key: "job:nightly-report", Ttl: time.Hour},
			Job:      cron.As(report.NewNightlyJob),
		},
		{Name: "heartbeat", Schedule: "@every 30s", Job: cron.As(NewHeartbeat)},
	},
}
```

| `JobConfig` field | Description |
| --- | --- |
| `Name` | Operation name, as `op_name` in logs. |
| `Schedule` | Standard 5-field spec (`minute hour day-of-month month day-of-week`), a descriptor (`@yearly`, `@monthly`, `@weekly`, `@daily`, `@hourly`), or `@every <duration>`. Prefix with `CRON_TZ=<zone>` for a time zone; the default is the process's local time. |
| `Lock` | Optional distributed lock, so a job runs on one replica at a time. Requires an `operation.ILocker` provider; without one, the server fails at startup. A replica that loses the race skips the run. |
| `Job` | `cron.As(constructor)`. The constructor's dependencies are resolved by bootstrap, and it returns a `cron.IJob`. |

```go
type IJob interface {
	Tolerance() operation.Tolerance
	Run(ctx context.Context) error
}

type ExpireJob struct{ carts *cart.Service }

func NewExpireJob(carts *cart.Service) *ExpireJob { return &ExpireJob{carts: carts} }

func (j *ExpireJob) Tolerance() operation.Tolerance {
	return operation.Tolerance{Retry: true, RetryAttempts: 3, RetryDelay: 10 * time.Second}
}

func (j *ExpireJob) Run(ctx context.Context) error {
	return j.carts.ExpireStale(ctx)
}
```

Notes:

- Return `operation.Tolerance{}` for no retries, or `operation.DefaultTolerance` for 5 attempts, 5s apart.
- Retries re-run everything inside the `Tolerance` middleware, including server middlewares. The lock is held across all attempts. One `operation started`/`completed` pair is logged per run.
- Jobs receive a context that is **not** cancelled on shutdown. `Terminate` stops scheduling and waits for running jobs. Give long jobs their own timeouts and size `TermTimeout` accordingly.
- Cron has no per-job middlewares. Use server middlewares, or compose inside the job.

---

## Worker

Broker-agnostic message handling, made of two servers:

- **`worker.ConsumerConfig`** consumes messages from a broker and dispatches them to typed handlers. An **inbox** table makes processing idempotent, and attempts are counted per subscriber.
- **`worker.ProducerConfig`** is an **outbox** relay. It publishes messages your application stored in an outbox table, in the same transaction as the business change, so nothing is lost or published for a rolled-back change.

They can run in one app or in separate deployments.

### Messages

```go
type Message struct {
	ID          string                 // unique id (UUIDv7 from NewMessage)
	Name        string                 // routes to the handler (consumer) and publisher (producer)
	Payload     []byte                 // JSON body
	Headers     *types.JSONMap[string] // string headers
	CreatedAt   time.Time
	Key         string // broker routing / partition / ordering key
	Destination string // where to publish, e.g. an exchange
}

msg := worker.NewMessage("order.placed", orderID, "orders", payload)
```

Broker adapters carry identity across the wire with the headers `worker.MessageIDHeader` (`x-msg-id`), `worker.MessageNameHeader` (`x-msg-name`) and `worker.MessageTimestampHeader` (`x-msg-timestamp`, Unix nanoseconds). On the consuming side they rebuild the message with `worker.NewMessageFromDelivery`.

### Consumer

```go
var Consumer = worker.ConsumerConfig{
	Subscribers: []worker.SubscriberConfig{
		{
			Name:        "billing", // the broker's subscriber name
			Concurrency: 8,
			MaxAttempt:  5,
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewTenantMiddleware)},
			Handlers: []worker.HandlerConfig{
				{
					Name:    "order.placed", // Message.Name this handler receives
					Handler: worker.HandlerFor((*billing.Service).OnOrderPlaced),
				},
				{
					Name:                "order.cancelled",
					Lock:                operation.Lock{Key: "order-cancelled", Ttl: time.Minute},
					TransactionDisabled: true,
					Handler:             worker.HandlerAs[billing.OrderCancelled](billing.NewCancelHandler),
				},
			},
		},
	},
}
```

Handlers have one signature:

```go
type Handler[Req any] func(ctx context.Context, req Req) error

func (s *Service) OnOrderPlaced(ctx context.Context, evt OrderPlaced) error { ... }

worker.HandlerFor((*billing.Service).OnOrderPlaced)               // method of a provided service
worker.HandlerAs[billing.OrderCancelled](billing.NewCancelHandler) // constructor returning worker.Handler[Req]
```

| Field | Default | Description |
| --- | --- | --- |
| `ConsumerConfig.ServerConfig` | | Termination, server-wide middlewares. |
| `ConsumerConfig.Binder` | JSON + validator | `worker.IBinder` that decodes `Message` into `Req`. |
| `ConsumerConfig.Clean` | see below | Inbox cleanup. |
| `SubscriberConfig.Name` | | Passed to `IBroker.NewSubscriber`. |
| `SubscriberConfig.Concurrency` | `1` | Messages handled in parallel. |
| `SubscriberConfig.MaxAttempt` | `3` | Attempts before a message is marked failed and rejected. |
| `SubscriberConfig.AckTimeout` | `3s` | Time limit for each acknowledgement sent to the broker: complete, retry or reject. |
| `SubscriberConfig.Middlewares` | | Middlewares for every handler of the subscriber. |
| `HandlerConfig.Name` | | The `Message.Name` the handler receives. |
| `HandlerConfig.Lock` | | Per-message lock. The key becomes `<Key>-<subscriber>-<messageID>`. If another instance holds it, the delivery is acknowledged and skipped. |
| `HandlerConfig.TransactionDisabled` | `false` | Skip the `Transactor`. |
| `HandlerConfig.Middlewares` | | Handler middlewares. |
| `HandlerConfig.Args` | | Static values copied into `ctx.Args`. Supports `worker.ArgKeyRecorderDisabled`. |

**Required dependencies:** `worker.IBroker` and `worker.InboxStorage`. `operation.Transactor` is needed unless every handler sets `TransactionDisabled`. `operation.ILocker` is needed only when a handler sets `Lock`. A missing dependency fails at startup and names the handler that needs it.

**Processing a message:**

1. The subscriber receives a delivery and picks the handler by `Message.Name`. An unknown name is rejected (no redelivery) and logged with code `nf_mh`.
2. Base middlewares run: `ID`, `Logger` (adds `msg_id`), `Locker`, `Recorder` (logs headers and payload, then `msg_elapsed` since `CreatedAt`), `Recovery`, and the **idempotency guard**. The guard looks the message up in the inbox by `(message ID, subscriber)`. A message already completed, failed or rejected is acknowledged and skipped.
3. Server, subscriber and handler middlewares run.
4. The inbox attempt counter is incremented. A new message is recorded as *waiting*.
5. The payload is bound into `Req`. On failure the message is marked *rejected* and rejected at the broker, with no retry (`msg_bind`).
6. `Transactor.Do` runs your handler, then marks the inbox row *completed*. Both run on the transaction's context.
7. **Success:** the delivery is acknowledged. **Error or panic:** while attempts are below `MaxAttempt`, the delivery is retried through the broker's redelivery. At `MaxAttempt` it is marked *failed* and rejected.

### Producer (outbox relay)

Write outgoing messages to the outbox in the **same transaction** as the state change that produced them. Writing the row is the application's job:

```go
func (s *OrderService) Place(ctx context.Context, req PlaceReq) (PlaceResp, error) {
	order := newOrder(req)
	payload, err := json.Marshal(OrderPlaced{OrderID: order.ID})
	if err != nil {
		return PlaceResp{}, err
	}
	msg := worker.NewMessage("order.placed", order.ID, "orders", payload)

	err = s.tx.Do(ctx, func(ctx context.Context) error {
		if err := s.orders.Insert(ctx, order); err != nil {
			return err
		}
		return s.outbox.Insert(ctx, msg) // your INSERT INTO outbox_events
	})
	if err != nil {
		return PlaceResp{}, err
	}
	return PlaceResp{ID: order.ID}, nil
}
```

Then run the producer:

```go
var Producer = worker.ProducerConfig{
	Limit:            200,
	Interval:         500 * time.Millisecond,
	ClaimTTL:         time.Minute,
	DefaultPublisher: worker.PublisherConfig{Name: "events", Concurrency: 4},
	Publishers: []worker.PublisherConfig{
		{Name: "order.placed", Concurrency: 8, Timeout: 2 * time.Second},
	},
	ErrorPercentage: 50,
	OnErrorPercentage: func(ctx context.Context) {
		logger.FromCtx(ctx).Warn().Msg("outbox publish error rate is high")
	},
}
```

| Field | Default | Description |
| --- | --- | --- |
| `Limit` | `100` | Messages claimed per batch. |
| `Interval` | `1s` | Pause when the last batch was not full. Full batches are processed back to back. |
| `ClaimTTL` | `1m` | How long a claim belongs to one producer instance. Older claims can be taken over, for example after a crash. |
| `DefaultPublisher` | name `default`, concurrency `1`, timeout `3s` | Publishes messages whose `Name` matches no entry in `Publishers`. Its broker publishers come from `IBroker.NewDefaultPublisher`; `Name` only labels its logs. |
| `Publishers` | | Dedicated publishers. A message goes to the publisher whose `Name` equals `Message.Name`; the same name is passed to `IBroker.NewPublisher`. `Concurrency` is the number of broker publishers, `Timeout` the per-publish timeout. |
| `ErrorPercentage`, `OnErrorPercentage` | off | When a batch's failure rate reaches `ErrorPercentage`, `OnErrorPercentage` is called. Without a callback, the breach is logged with `err_pct`. |
| `Clean` | see below | Deletion of processed outbox rows. |

**Required dependencies:** `worker.IBroker` and `worker.OutboxStorage`.

Each cycle **claims** up to `Limit` unprocessed messages, publishes them in parallel, then **completes** the claim. Published messages are marked processed, and failed ones are released for the next cycle. Delivery is *at least once*; the consumer's inbox makes handling idempotent.

### Cleanup

Both servers periodically delete old rows:

```go
type CleanConfig struct {
	MaxTTL          time.Duration // age before deletion; default 30 days; negative disables
	CleanupLimit    int           // rows per delete batch; default 100
	CleanupInterval time.Duration // default 10 minutes
}
```

The consumer deletes *completed* and *rejected* inbox rows; *failed* rows are kept for inspection. The producer deletes processed outbox rows.

### Storage contracts

```go
type OutboxStorage interface {
	// Atomically claim up to claim.Limit rows with processed_at IS NULL and
	// (processing_id IS NULL OR processing_at < claim.ExpiredBefore), using
	// FOR UPDATE SKIP LOCKED; set processing_id/processing_at; return them.
	Claim(ctx context.Context, claim worker.Claim) ([]worker.Message, error)
	// Atomically mark successfulIDs processed and release every other row of claimID.
	Complete(ctx context.Context, claimID string, successfulIDs []string) error
	// Delete processed rows older than before, at most limit rows.
	DeleteProcessedBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}

type InboxStorage interface {
	Status(ctx context.Context, messageID, subscriberName string) (worker.InboxStatus, bool, error)
	IncrementAttempt(ctx context.Context, messageID, subscriberName string) (int, error)
	MarkCompleted(ctx context.Context, messageID, subscriberName string) error
	MarkFailed(ctx context.Context, messageID, subscriberName string) error
	MarkRejected(ctx context.Context, messageID, subscriberName string) error
	DeleteBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}
```

`InboxStatus` values are `InboxWaiting` (0), `InboxCompleted` (1), `InboxFailed` (2) and `InboxRejected` (3).

The SQL outbox and inbox adapters work on these tables. A PostgreSQL example:

```sql
CREATE TABLE outbox_events (
    id            TEXT PRIMARY KEY,           -- Message.ID; UUIDv7 keeps claim order chronological
    name          TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    headers       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    message_key   TEXT        NOT NULL DEFAULT '',
    destination   TEXT        NOT NULL DEFAULT '',
    processing_id TEXT,
    processing_at TIMESTAMPTZ,
    processed_at  TIMESTAMPTZ
);
CREATE INDEX outbox_events_pending ON outbox_events (processed_at, processing_at);

CREATE TABLE inbox_events (
    message_id      TEXT        NOT NULL,
    subscriber_name TEXT        NOT NULL,
    status          SMALLINT    NOT NULL,
    attempts        INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (message_id, subscriber_name)
);
```

### Broker contracts

To support a new broker, implement these and expose the broker as a resource whose `Value()` is the `IBroker`:

```go
type IBroker interface {
	Name() string
	NewDefaultPublisher() (IPublisher, error)
	NewPublisher(name string) (IPublisher, error)
	NewSubscriber(name string) (ISubscriber, error)
}

type IPublisher interface {
	Publish(ctx context.Context, msg Message) error // blocking, single message
	Close() error
}

type ISubscriber interface {
	// Consume blocks. Call cs.Handler(ctx, delivery) for every message, with at
	// most cs.Concurrency in flight. Stop on ctx cancellation and wait for every
	// started handler before returning.
	Consume(ctx context.Context, cs ConsumerState) error
	Close() error
}

type IDelivery interface {
	Message() Message
	Complete(ctx context.Context) error // ack
	Retry(ctx context.Context) error    // nack, redeliver
	Reject(ctx context.Context) error   // drop, no redelivery
}
```

`process.ConcurrentRunner` handles the concurrency limit and the wait in `Consume`.

---

## Custom servers

Anything with a lifecycle can be a forgo server. Implement two interfaces:

```go
type ServerFactory interface {
	Name() string
	New(r forgo.Resolver) (forgo.IServer, error) // resolve everything here
}

type IServer interface {
	Name() string
	Term() forgo.TermConfig
	Init(ctx context.Context) error      // prepare; ctx carries the server logger
	Serve() error                        // block until stopped; return nil on graceful stop
	Terminate(ctx context.Context) error // graceful stop; ctx expires after TermTimeout
	Kill()                               // forced stop when Terminate times out
}
```

Contract:

- `New` is the composition root. Resolve handlers and middlewares through the `Resolver` here, because it is sealed before `Serve`.
- `Init` builds what `Serve` needs and does not block. Store `ctx` to keep the logger and trace fields.
- `Serve` blocks. Returning an error stops the whole app. Return nil after a graceful stop.
- `Terminate` makes `Serve` return and waits for in-flight work, respecting `ctx`.
- Run work as operations through `operation.Chain`, so every forgo middleware works on your server.

A complete example: a server that runs a task at a fixed interval.

```go
package ticker

import (
	"context"
	"time"

	"forgo"
	"forgo/middleware"
	"forgo/operation"
	"forgo/types"
)

const ServerName = "ticker"

type Task interface {
	Run(ctx context.Context) error
}

// Config is the ServerFactory. Embedding forgo.ServerConfig adds
// Term, Middlewares and DisableBaseMiddlewares.
type Config struct {
	forgo.ServerConfig
	TaskName string
	Interval time.Duration
	Task     any // constructor returning a Task, resolved by bootstrap
}

func (c Config) Name() string { return ServerName }

func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	sc, err := c.ServerConfig.Sanitize().ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	task, err := r.Call[Task](c.Task)
	if err != nil {
		return nil, err
	}
	c.ServerConfig = sc
	return &Server{cfg: c, task: task}, nil
}

type Server struct {
	cfg     Config
	task    Task
	ctx     context.Context
	handler operation.Handler
	stop    chan struct{}
	done    chan struct{}
}

func (s *Server) Name() string           { return ServerName }
func (s *Server) Term() forgo.TermConfig { return s.cfg.Term }

func (s *Server) Init(ctx context.Context) error {
	s.ctx = ctx
	s.stop, s.done = make(chan struct{}), make(chan struct{})

	var mws []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		mws = append(mws,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Recovery(nil),
		)
	}
	mws = append(mws, s.cfg.ResolvedMiddlewares()...)

	s.handler = operation.Chain(func(ctx *operation.Context) error {
		return s.task.Run(ctx)
	}, mws...)
	return nil
}

func (s *Server) Serve() error {
	defer close(s.done)
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return nil
		case <-t.C:
			_ = s.handler(&operation.Context{
				Context: s.ctx,
				Name:    s.cfg.TaskName,
				Type:    "tick",
				Args:    types.NewMap[any, any](),
			})
		}
	}
}

func (s *Server) Terminate(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Kill() {}
```

```go
forgo.WithServers(ticker.Config{TaskName: "heartbeat", Interval: 30 * time.Second, Task: NewHeartbeat})
```

`ServerConfig` helpers:

| Method | Description |
| --- | --- |
| `Sanitize() ServerConfig` | Applies defaults (`TermTimeout` 30s). |
| `ResolveMiddlewares(r) (ServerConfig, error)` | Resolves `Middlewares` and returns a copy that holds them. |
| `ResolvedMiddlewares() []operation.Middleware` | The resolved server middlewares. |

`forgo.ResolveMiddlewares(r, factories)` resolves any other list of `MiddlewareFactory`, such as route-level middlewares.

---

## CLI

The CLI runs **one command per invocation**, with the same resources, providers and middleware model as an App. Where an App runs servers until it is stopped, a Cli bootstraps, runs a single command as an operation, waits for its background goroutines, and returns.

```go
func main() {
	c := cli.New(
		cli.WithName("shop-cli"),
		cli.WithLogger(config.Logger),
		cli.WithResources(config.DB),
		cli.WithProviders(config.Adapters...),
		cli.WithProviders(config.Services...),
		cli.WithConfig(cli.Config{
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewMetricsMiddleware)},
			Commands: []cli.CommandConfig{
				{Name: "migrate", Command: cli.As(migration.NewDefaultMigrateCommand)},
				{Name: "openapi", Command: cli.As(openapi.NewDefaultOpenAPICommand(config.HTTP))},
				{
					Name:    "reindex",
					Lock:    operation.Lock{Key: "cmd:reindex", Ttl: time.Hour},
					Command: cli.As(search.NewReindexCommand),
				},
			},
		}),
	)
	if err := c.Run(); err != nil {
		os.Exit(1)
	}
}
```

```console
$ shop-cli migrate -a=up
$ shop-cli openapi -o=./docs/openapi.json
$ shop-cli reindex -index=products -dry-run
```

### Writing a command

```go
type Command interface {
	Init(*flag.FlagSet)        // declare flags
	Validate() error           // check parsed flags
	Run(context.Context) error // do the work
}

type CommandFactory func() Command
```

A command is declared with a **constructor** that receives dependencies and returns a `CommandFactory`. The factory builds a fresh `Command` for each invocation, because commands hold flag state.

```go
type ReindexCommand struct {
	search *search.Service
	index  string
	dryRun bool
}

func NewReindexCommand(svc *search.Service) cli.CommandFactory {
	return func() cli.Command { return &ReindexCommand{search: svc} }
}

func (c *ReindexCommand) Init(fs *flag.FlagSet) {
	fs.StringVar(&c.index, "index", "", "index to rebuild (required)")
	fs.BoolVar(&c.dryRun, "dry-run", false, "only report changes")
}

func (c *ReindexCommand) Validate() error {
	if c.index == "" {
		return errors.New("-index is required")
	}
	return nil
}

func (c *ReindexCommand) Run(ctx context.Context) error {
	return c.search.Reindex(ctx, c.index, c.dryRun)
}
```

### What `Run` does

1. Creates the logger with `app_name`.
2. Looks up `os.Args[1]` among the commands. An unknown or missing name returns an error that lists the available commands, before anything is bootstrapped.
3. Builds the bootstrap. Every resource and provider is created, even for commands that don't use them.
4. Resolves the optional `operation.ILocker`, the global and command middlewares, and the command constructor. Then seals the bootstrap.
5. Runs the command as an operation (`Type: "command"`, `Name: <command>`) through `ID` → `Logger` → `Locker` → `Recorder` → `Recovery`, then the global middlewares, then the command middlewares.
6. Inside the chain: creates the command from its factory, calls `Init` with a fresh `flag.FlagSet`, parses `os.Args[2:]`, then calls `Validate` and `Run`. Parse failures carry code `command_parse` and message `invalid arguments: …`. Validation failures carry code `command_validate` and message `invalid command: …`.
7. Waits for goroutines started with `process.Go`, closes resources, and returns the command's error.

The command's context is cancelled on SIGINT or SIGTERM, so a long-running command can stop cleanly. A second signal terminates the process immediately.

`Run` returns the error instead of exiting, so `main` chooses the exit code after cleanup. `CommandConfig.Lock` keeps a command from running concurrently across machines. It needs an `operation.ILocker` provider; without one, `Run` fails before the command starts.

---

## Built-in commands

### `migrate`: SQL migrations

Package `forgo/commands/migration` runs SQL migrations on PostgreSQL and MySQL. It uses [sql-migrate](https://github.com/rubenv/sql-migrate) file annotations and sources, with its own context-aware planner and executor.

**Default command**, which needs exactly one `*sql.DB` in the bootstrap:

```go
{Name: "migrate", Command: cli.As(migration.NewDefaultMigrateCommand)}
```

It detects the dialect from the driver (`github.com/lib/pq` → postgres, `github.com/go-sql-driver/mysql` → mysql), reads `./migrations`, tracks applied migrations in the table `migrations`, and takes an advisory lock so concurrent runners serialize.

**Custom setup**, for another directory, an embedded source, a named database, a timeout, or another table:

```go
//go:embed migrations/*.sql
var migrationFiles embed.FS

type MigrateDeps struct {
	DB *sql.DB `boot:"primary"`
}

func NewMigrateCommand(d MigrateDeps) (cli.CommandFactory, error) {
	m, err := migration.New(
		d.DB,
		&sqlmigrate.EmbedFileSystemMigrationSource{FileSystem: migrationFiles, Root: "migrations"},
		migration.NewPostgresLocker(7412345),             // or migration.NewMySQLLocker("shop:migrations")
		migration.Config{
			Dialect: "postgres",                          // "postgres" | "mysql"
			Schema:  "public",                            // optional; must exist
			Table:   "schema_migrations",                 // default "migrations"
			Timeout: 10 * time.Minute,                    // whole operation incl. lock wait; 0 = none
		},
	)
	if err != nil {
		return nil, err
	}
	creator := migration.NewCreator("./migrations")
	return func() cli.Command { return migration.NewCommand(m, creator, os.Stdout) }, nil
}
```

**Usage:**

```console
$ app migrate -a=up                       # apply all pending migrations
$ app migrate -a=up -steps=2              # apply the next 2
$ app migrate -a=down                     # roll back the last applied migration
$ app migrate -a=down -steps=3            # roll back 3
$ app migrate -a=status                   # ID / STATE / APPLIED AT table
$ app migrate -a=plan                     # show the SQL "up" would run
$ app migrate -a=plan -direction=down -steps=2
$ app migrate -a=new -name=create_users   # create a timestamped template
$ app migrate -a=new -name=create_users -dir=./db/migrations
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `-a`, `-action` | all | `up`, `down`, `status`, `plan`, `new` (required) |
| `-steps` | `up`, `down`, `plan` | Up: `0` (default) means all. Down: defaults to 1; an explicit value must be positive. |
| `-direction` | `plan` | `up` (default) or `down` |
| `-name` | `new` | Migration name: letters, digits and underscores, starting with a letter |
| `-dir` | `new` | Override the target directory |

Flags that don't apply to the chosen action are rejected, as are positional arguments.

**Migration files** use sql-migrate annotations. `new` creates `<yyyyMMddHHmmssSSS>_<name>.sql`:

```sql
-- +migrate Up
CREATE TABLE users (
    id    BIGINT       NOT NULL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
CREATE UNIQUE INDEX idx_users_email ON users (email);

-- +migrate Down
DROP TABLE users;
```

Wrap statements containing semicolons in `-- +migrate StatementBegin` / `-- +migrate StatementEnd`. Use `-- +migrate Up notransaction` (or `Down notransaction`) for statements that cannot run inside a transaction.

**Behavior:**

- `up` and `down` take the advisory lock on one pinned connection. All SQL, tracking writes and the lock share that session. The tracking table is created on first use.
- Each migration runs in its own transaction, including its tracking row, unless it is marked `notransaction`. PostgreSQL DDL is transactional. MySQL DDL may commit implicitly.
- `down` only rolls back applied migrations, in reverse order.
- `status` and `plan` are read-only. They take no lock and create no table.
- Applied migrations missing from the source show as `unknown` in `status`, and block `up`, `down` and `plan`.
- Treat applied files as immutable; there are no checksums.
- MySQL DSNs need `parseTime=true` (and `loc=UTC` is recommended). PostgreSQL advisory locks need a direct connection or session pooling, not transaction pooling.
- On failure the error names the migration and the statement number. Migrations completed before the failure stay applied and are reported.

### `openapi`: API documents

Package `forgo/commands/openapi` generates **OpenAPI 3.1** documents straight from your `http.Config`. It never starts a server or calls handlers.

```go
{Name: "openapi", Command: cli.As(openapi.NewDefaultOpenAPICommand(config.HTTP))}
```

With your own title and description:

```go
func NewOpenAPICommand() cli.CommandFactory {
	return func() cli.Command {
		return openapi.NewCommand(config.HTTP, openapi.WithInfo(openapi.Info{
			Title:       "Shop API",
			Description: "Public HTTP API of the shop",
		}))
	}
}
```

```console
$ app openapi -o=./docs/openapi.json
$ app openapi -o=./docs/openapi.yaml
$ app openapi -output=./docs/api.yml -format=yaml
```

**One document per route version.** The version is appended to the output name: `-o=./docs/openapi.json` with versions `v1` and `v2` writes `./docs/openapi-v1.json` and `./docs/openapi-v2.json`. Each document contains only its version's routes and schemas, and `info.version` is the route version. `-format` defaults to the output extension (`.yaml`/`.yml` → YAML, otherwise JSON). Files are replaced atomically, and the parent directory must exist.

**What is documented, for each route:**

- `operationId` from `Name`, the description from `Description`, and the path including the version prefix.
- **Parameters and body** from the request type, using one source per field with the precedence `uri > header > query > json`:
  - `uri` fields become required path parameters and must match the path wildcards exactly.
  - `header` and `query` fields become parameters. Query arrays use `form`/`explode`.
  - `json` fields form the request body schema.
  - `"-"` disables a source.
- **Validation hints** from `validate` tags: `required`, `min`, `max`, `len`, `gt`, `gte`, `lt`, `lte`, `oneof`, `email`, `uuid*`, `hostname`, `url`, `uri`, `ip`, `ipv4` and `ipv6` become schema constraints. Rules after `dive` are not applied to the collection itself.
- **Response schema** from the response type, placed inside your actual serializer's envelope. The generator probes the configured `Serializer` (default or custom), finds where the result lands, and copies the status, media type and response headers it observes.
- A `default` error response, from probing the serializer's `Error`.
- `EmptyHandler` routes have no request and no result.

**Supported types:** primitives, pointers (nullable), slices, arrays, maps with JSON-compatible keys, nested and recursive structs (as `components/schemas`), embedded structs with JSON dominance rules, `time.Time`, `json.Number`, `json.RawMessage`, and `encoding.TextMarshaler`. A type with a custom `json.Marshaler` fails generation, because its shape cannot be inferred.

**Programmatic use:**

```go
gen := openapi.New(config.HTTP.Sanitize(), openapi.WithInfo(openapi.Info{Title: "Shop API"}))
docs, err := gen.GenerateAll(ctx)                           // map[version]*openapi.Document
err = openapi.WriteFiles(ctx, "./docs/openapi.json", "json", docs)
data, err := openapi.Marshal(docs["v1"], "yaml")
```

---

## Logging

Forgo logs with [zerolog](https://github.com/rs/zerolog): structured, leveled and allocation-free. Every log line written through the context carries the full trace of where it came from.

### Configuration

```go
var Logger = logger.Config{
	Level:  zerolog.InfoLevel, // zero value is DebugLevel
	Caller: true,              // add file:line
	StaticFields: map[string]string{
		"env":     forgo.Env("ENV", "local"),
		"version": version,
	},
	Console: logger.ConsoleConfig{Enabled: true, NoColor: false},
	File: logger.FileConfig{
		Enabled:   true,
		NameFunc:  func(logger.FileConfig) string { return "logs/app.log" }, // default "log"
		MaxSize:   100, // MB before rotation
		MaxBackup: 7,   // rotated files kept
		MaxAge:    30,  // days
		Compress:  true,
	},
}
```

| Outputs enabled | Result |
| --- | --- |
| none | JSON lines to stdout (production default) |
| `Console` | human-readable output to stdout |
| `File` | rotating JSON files |
| both | both at once |

Timestamps are RFC 3339 with microseconds and the zone offset, for example `2026-10-07T23:17:54.168989+03:00`. Run the process with `TZ=UTC` for `Z` timestamps.

### Contextual fields

Each layer adds its fields to the logger it passes down:

| Layer | Fields |
| --- | --- |
| App | `app_name`, `app_id` (CLI: `app_name`) |
| Server | `server_name`, `server_id` |
| Worker internals | `broker`, `sub`, `sub_id` / `pub`, `pub_id` |
| Operation | `op_type`, `op_name`, `op_id` (+ `msg_id` for messages) |
| Recorder | `elapsed`, `code`, `severity`, `error`, plus transport fields |

### Logging from business code

Always take the logger from the context. It already carries every field above:

```go
func (s *OrderService) Place(ctx context.Context, req PlaceReq) (PlaceResp, error) {
	logger.FromCtx(ctx).Info().Str("sku", req.SKU).Msg("placing order")
	// ...
}
```

To add fields for everything downstream, derive a logger and put it back into the context:

```go
lg := logger.FromCtx(ctx).With().Str("order_id", order.ID).Logger()
ctx = logger.NewCtx(ctx, lg)
```

| Function | Description |
| --- | --- |
| `logger.FromCtx(ctx) *zerolog.Logger` | The context's logger, or `logger.DefaultCtxLogger` (JSON to stdout) if none. |
| `logger.NewCtx(ctx, zerolog.Logger) context.Context` | Store a logger in a context. |
| `logger.FieldAdder` | `func(*operation.Context, zerolog.Context) zerolog.Context`, used by `middleware.Logger`. |
| `logger.EventFields` | `func(*operation.Context, *zerolog.Event)`, used by `middleware.Recorder`. |

All log keys used by forgo are constants in `forgo/logger` (`logger.OperationIDLogKey`, `logger.ErrorSeverityLogKey`, …). Use them when you query or extend the logs.

When logging errors yourself, follow the framework's convention and add the severity:

```go
logger.FromCtx(ctx).Error().Err(err).
	Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
	Msg("payment provider unreachable")
```

---

## Background goroutines

Use **`process.Go`** instead of a bare `go func()` to spawn an untracked goroutine from business logic.

```go
func (s *OrderService) Place(ctx context.Context, req PlaceReq) (PlaceResp, error) {
	order, err := s.create(ctx, req)
	if err != nil {
		return PlaceResp{}, err
	}

	bg := context.WithoutCancel(ctx) // keep logger and values; drop the request's cancellation
	process.Go(func() {
		if err := s.mailer.SendConfirmation(bg, order); err != nil {
			logger.FromCtx(bg).Error().Err(err).Msg("send confirmation")
		}
	})

	return PlaceResp{ID: order.ID}, nil
}
```

**Why:** tracked goroutines are the only ones an App or Cli can wait for. On shutdown, the App terminates every server, then calls `process.WaitGoroutines()`, and only then closes resources. A bare `go func()` can be cut off mid-work when the process exits, or run against a database that has already been closed.

Guidelines:

- The wait has no timeout. Background work must finish on its own, so give it a deadline (`context.WithTimeout(bg, …)`).
- Panics in the goroutine are not recovered. Recover inside it if the work can panic.
- An HTTP request's context is cancelled when the request ends. Use `context.WithoutCancel(ctx)` for work that outlives the request.

Bounded concurrency:

| Type | Behavior |
| --- | --- |
| `process.NewRunner(sem)` | `DoAsync(fn)` starts `fn` as a tracked goroutine if a slot is free and returns `process.ErrNoSlot` otherwise. `TryAsync(fn)` falls back to running `fn` synchronously when no slot is free. `Do(fn)` runs synchronously. |
| `process.NewConcurrentRunner(sem)` | `Run(ctx, fn)` blocks until a slot frees up (or `ctx` ends) and starts `fn`. `Wait()` waits for every started `fn`. It uses its own wait group and is **not** tracked by `process.WaitGoroutines`. |
| `process.NewChannelSemaphore(n)` | `ISemaphore` with `Acquire`, `WaitAcquire(ctx)` and `Release`. |

```go
var mailRunner = process.NewRunner(process.NewChannelSemaphore(20))

mailRunner.TryAsync(func() { _ = mailer.Send(bg, msg) })
```

---

## Utilities

### `types.Map` and `types.JSONMap`

```go
m := types.NewMap[string, int]()                // or types.NewMapWithData(map[string]int{...})
m.Set("a", 1)
v, ok := m.Get("a")
m.Delete("a")
snapshot := m.Clone()                           // shallow copy of the underlying map

n := m.GetTyped[int]("a")                       // typed read; zero value if absent or mismatched
```

`GetTyped[T]` also converts `json.Number` values to sized integer and float types without losing precision.

`types.JSONMap[V]` is a `Map[string, V]` that implements `driver.Valuer` and `sql.Scanner`. It stores as JSON and decodes numbers as `json.Number`. A nil map is SQL `NULL`. Message headers use it.

```go
meta := types.NewJSONMap[any]()
_, err := db.ExecContext(ctx, `INSERT INTO events (metadata) VALUES ($1)`, meta)
err = db.QueryRowContext(ctx, `SELECT metadata FROM events WHERE id = $1`, id).Scan(meta)
```

### `forgo.Env`

```go
addr := forgo.Env("HTTP_ADDR", ":8080") // value of HTTP_ADDR, or ":8080" if unset or empty
```

### `transport`

| Type | Description |
| --- | --- |
| `transport.ISerializer` | Response serializer contract (see [HTTP responses](#responses)). |
| `transport.IConnObserver` | Hooks for accept, read and write errors on server connections. |
| `transport.LoggingObserver` | Default observer. Logs accept errors except `net.ErrClosed`. |
| `transport.Listener`, `transport.Conn` | `net.Listener` and `net.Conn` wrappers that report to an observer. |

---

## Adapters

The forgo adapter library provides ready-made resources and providers for common infrastructure, so you only configure them:

| Adapter | Kind | Provides | Notes |
| --- | --- | --- | --- |
| `sqldb` | resource | `*sql.DB` | `Config{Name, DriverName, DSN, MaxOpenConns, MaxIdleConns, ConnMaxLifetime, ConnMaxIdleTime, ConnectTimeout}`. Pings on startup. Import your SQL driver in `main`. |
| `rabbitmq` | resource | `worker.IBroker` | `Config{Name, Addr, Port, Username, Password, VHost, Exchanges, Queues, Publishers, Subscribers}`. Declares exchanges, queues and bindings at startup. Named publishers and subscribers match worker config names. Publishes to `Message.Destination` (exchange) with `Message.Key` (routing key). Ack/nack/reject map to `Complete`/`Retry`/`Reject`. |
| `outbox` (Postgres, MySQL) | provider | `worker.OutboxStorage` | Uses the `outbox_events` table. Claims with `FOR UPDATE SKIP LOCKED`. |
| `inbox` (Postgres, MySQL) | provider | `worker.InboxStorage` | Uses the `inbox_events` table. Status transitions are idempotent. |
| `locker` (Redis) | provider | `operation.ILocker` | `SET NX` with TTL. |

Adapters are ordinary resources and providers. Anything you write yourself plugs in the same way: implement `forgo.ResourceFactory`, or write a constructor and list it in `WithProviders`.

---

## Error code reference

| Code | Origin | Status | Meaning |
| --- | --- | --- | --- |
| `x` | any | Unknown → HTTP 500 | Unmapped error (`operation.NewFallbackError`) |
| `nf` | HTTP | 404 | No route matches the request |
| `eof` | HTTP | 400 | Request body is empty or truncated |
| `json_syntax` | HTTP | 400 | Malformed JSON body |
| `json_type` | HTTP | 400 | JSON value has the wrong type |
| `binding` | HTTP | 400 | Query, path or header value could not be bound |
| `validation` | HTTP | 400 | `validate` rule failed |
| `msg_bind` | worker | InvalidArgument | Payload could not be decoded or validated; message rejected |
| `msg_ack` | worker | Internal | Acknowledging the delivery failed |
| `nf_mh` | worker | InvalidArgument | No handler for the message name; message rejected |
| `command_parse` | CLI | InvalidArgument | Command flags could not be parsed |
| `command_validate` | CLI | InvalidArgument | `Command.Validate` failed |

Bootstrap errors are returned or panicked before anything serves. They start with `bootstrap:` and name the provider, argument and type involved. Check them with `errors.Is` against `forgo.ErrMissingDependency`, `forgo.ErrAmbiguousDependency` and `forgo.ErrResourceNotFound`.
