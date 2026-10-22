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
- [gRPC server](#grpc-server)
- [Cron server](#cron-server)
- [Worker](#worker)
- [Custom servers](#custom-servers)
- [CLI](#cli)
- [Built-in commands](#built-in-commands)
- [Testing](#testing)
- [Logging](#logging)
- [Metrics](#metrics)
- [Background goroutines](#background-goroutines)
- [Live configs](#live-configs)
- [Utilities](#utilities)
- [Adapters](#adapters)
- [Error code reference](#error-code-reference)

---

## Overview

Every backend needs the same technical scaffold: wiring dependencies, running servers, shutting down cleanly, logging with trace context, retrying, locking, migrating databases, documenting APIs. Forgo implements that scaffold. You describe your system as configuration and write the business code; forgo resolves and runs everything else.

### Building blocks

| Concept | What it is | Package |
| --- | --- | --- |
| **Resource** | A long-lived external connection or client whose lifetime matches the application: `*sql.DB`, a broker connection, a Redis client. Created by a `ResourceFactory`, closed on shutdown. | `github.com/efegreat/forgo` |
| **Provider** | A constructor for an object that *uses* resources or other providers to implement behavior: repositories, services, API clients. | `github.com/efegreat/forgo` |
| **Bootstrap** | The dependency resolver. Builds resources and providers, lets servers construct their handlers, then seals itself before anything runs. | `github.com/efegreat/forgo` |
| **Server** | A long-running unit that serves work: HTTP, gRPC, cron, worker, or your own. You declare it as a config value that implements `ServerFactory`. | `github.com/efegreat/forgo/server/...` |
| **App** | Runs one or more servers as a whole and manages their lifecycle: start, graceful termination, forced termination. | `github.com/efegreat/forgo` |
| **CLI** | Runs one named command per invocation, using the same resource/provider model as an App. | `github.com/efegreat/forgo/cli` |
| **Operation** | The transport-agnostic unit of work. An HTTP request, a gRPC call, a cron run, a consumed message and a CLI command are all operations. | `github.com/efegreat/forgo/operation` |
| **Middleware** | `func(operation.Handler) operation.Handler`. Because everything is an operation, a middleware works on every server. | `github.com/efegreat/forgo/middleware` |

> **Resource or provider?** If the object owns a long-lived external connection whose lifetime should match the application, it is a resource. If it merely uses resources or other dependencies to implement application behavior, it is a provider. `*sql.DB` is a resource; a `UserRepository` holding that `*sql.DB` is a provider.

### Lifecycle

Dependencies are dynamic while the application is being constructed and static once it starts running:

```text
forgo.NewApp(options...)          stores configuration only
app.Run()
 ├─ logger.Config.New()           the app logger, also the default of contexts without one
 ├─ Bootstrap.Build()             resources (in order) → providers (in order)
 ├─ for each server:
 │    ServerFactory.New(resolver) resolve handlers, middlewares, jobs, commands
 │    IServer.Init(ctx)           ctx carries the server's logger
 ├─ Bootstrap.Seal()              dependency resolution is switched off
 ├─ IServer.Serve()               every server in its own goroutine
 │        … until SIGINT/SIGTERM, or any Serve returns an error
 ├─ terminate every server in parallel
 │    TermSig.Signal() + wait → Terminate(ctx) → Kill() unless Terminate returns nil and Serve returns within TermTimeout
 ├─ process.WaitGoroutines()      wait for tracked background goroutines
 ├─ Bootstrap.Close()             resources closed in reverse creation order
 └─ return the failures           nil after a clean stop on a signal
```

No reflection or dependency lookup happens after `Seal`. Request handling runs pre-built handler chains only.

### Packages

| Import path | Contents |
| --- | --- |
| `github.com/efegreat/forgo` | `App`, `Bootstrap`, `Resolver`, `Resource`, `ServerFactory`, `IServer`, `ServerConfig`, `MiddlewareFactory`, `LiveConfig`, `LiveConfigMemoryTTL`, `LiveConfigs`, `LiveConfigStore`, `LiveConfigCache`, `LiveConfigCatalog`, `Env` |
| `github.com/efegreat/forgo/operation` | `Context`, `FromCtx`, `Handler`, `Middleware`, `Chain`, `Error`, `ErrorOf`, `Status`, `ErrorMapper`, `Lock`, `ILocker`, `IObserver`, `Observation`, `ILockObserver`, `ObserveLocks`, `Tolerance`, `Transactor` |
| `github.com/efegreat/forgo/middleware` | Built-in middlewares: `ID`, `Logger`, `Recorder`, `Observe`, `Recovery`, `Locker`, `Tolerance`, `Guard`, plus `PanicError`, `ErrRejected` and `ErrNoLocker` |
| `github.com/efegreat/forgo/server/http` | HTTP server, plus opt-in `CORS`, `RateLimit` and `JWTAuth` middlewares and live config admin handlers |
| `github.com/efegreat/forgo/server/grpc` | gRPC server, health checking and reflection services |
| `github.com/efegreat/forgo/server/cron` | Cron server |
| `github.com/efegreat/forgo/server/worker` | Message consumer and outbox producer servers, broker and storage contracts, settlement and relay observers |
| `github.com/efegreat/forgo/cli` | Command-line runner |
| `github.com/efegreat/forgo/commands/migration` | SQL migration command |
| `github.com/efegreat/forgo/commands/openapi` | OpenAPI 3.1 generator command |
| `github.com/efegreat/forgo/forgotest` | Test harness that runs the app of `main` for feature and E2E tests: `Env`, `IListenerServer`, `IHandlerServer`, `IJobServer`, `IConsumerServer`, `Broker`, `ReplaceResource` |
| `github.com/efegreat/forgo/logger` | Logger configuration, context propagation, log keys |
| `github.com/efegreat/forgo/redact` | The masking of `SensitiveFields` in logs, for code that logs input of its own |
| `github.com/efegreat/forgo/process` | Tracked goroutines, runners, semaphores |
| `github.com/efegreat/forgo/transport` | Serializer contract, connection observer contracts and connection states |
| `github.com/efegreat/forgo/types` | Concurrency-safe `Map`, and `JSONMap`, which encodes as JSON and stores in JSON columns |

### Requirements

Install forgo with `go get github.com/efegreat/forgo`. The [adapters](#adapters) are a separate module: `go get github.com/efegreat/forgo-adapter`.

- **Go 1.27 or newer.** Forgo uses generic methods, such as `r.Resolve[T]()` and `m.GetTyped[T](key)`.
- The package `github.com/efegreat/forgo/server/http` is named `http`. When a file also needs the standard library, alias one of them, for example `nethttp "net/http"`. Likewise, `github.com/efegreat/forgo/server/grpc` is named `grpc`, like grpc-go: alias it, for example `fgrpc`.

### Project layout

forgo imposes no project layout or architecture. Bootstrap matches constructors and interfaces by type, wherever they are declared, so layered, hexagonal or per-feature packages, or a single package, all work alike. The scaffold project shows one layout as an example, to copy, change or ignore.

Whatever the layout, three rules come from forgo itself:

- **Build each deployable's app in a function** that `main` and the tests both call, and read the configuration that tests change inside functions too ([The app as a function](#the-app-as-a-function)). Tests then run the wiring that production runs.
- **Declare providers after what they depend on.** Bootstrap calls them in declaration order and never sorts them ([Resolution rules](#resolution-rules)).
- **Give every requested type one match.** A parameter gets the one resource or provider assignable to it, and two are an error ([Resolution rules](#resolution-rules)), so an app provides one implementation of each interface its constructors request.

---

## Quick start

A small user API backed by PostgreSQL.

**Domain:** a service that depends on a repository interface it owns.

```go
// user/user.go
package user

import (
	"context"

	"github.com/efegreat/forgo/operation"
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

	"github.com/rs/zerolog"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo-adapter/resource/sqldb" // the SQL resource from forgo-adapter
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/server/http"

	"yourapp/user"
)

var Logger = logger.Config{
	Level:   zerolog.InfoLevel,
	Console: logger.ConsoleConfig{Enabled: true},
}

// DB is a function, not a var, so that tests can set DATABASE_URL before it is read (see Testing).
func DB() sqldb.Config {
	return sqldb.Config{
		Name:           "main",
		DriverName:     "postgres",
		DSN:            forgo.Env("DATABASE_URL", "postgresql://user:password@localhost/app?sslmode=disable"),
		ConnectTimeout: 5 * time.Second,
	}
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
	"os"

	_ "github.com/lib/pq" // database/sql driver

	"github.com/efegreat/forgo"
	"yourapp/config"
	"yourapp/postgresrepo"
	"yourapp/user"
)

func main() {
	app := forgo.NewApp(
		forgo.WithName("users-api"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB()),
		forgo.WithProviders(
			postgresrepo.NewUserRepository, // needs *sql.DB → the "main" resource
			user.NewService,                // needs user.Repository → *postgresrepo.UserRepository
		),
		forgo.WithServers(config.HTTP),
	)
	if err := app.Run(); err != nil {
		os.Exit(1) // Run logged the failure
	}
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

	"github.com/efegreat/forgo"

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
   bootstrap: provider 0 (yourapp/user.NewService): argument 0 (user.Repository): missing dependency user.Repository
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

5. **Failures are errors, not crashes.** A provider that returns an error, returns nil, or panics fails the build, and so does a resource factory. Panics are recovered and reported with a stack trace, and the resources created so far are closed. A `Resource.Close` that fails or panics does not keep the other resources from closing.

6. **Entry points are not providers.** HTTP handlers, middlewares, cron jobs, message handlers and commands are constructed by server factories through `Resolver.Call`. Their results are **not** registered, so nothing can depend on a handler.

### Designing the graph from top to bottom

Think of the graph in layers. Each layer only depends on the layers above it:

```text
Resources        *sql.DB, worker.IBroker, redis.UniversalClient    forgo.WithResources
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

Sentinel errors: `forgo.ErrMissingDependency`, `forgo.ErrAmbiguousDependency` and `forgo.ErrResourceNotFound`. Check them with `errors.Is`. Built-in servers use `ErrMissingDependency` to treat optional dependencies, such as `operation.ILocker` and `operation.IObserver`, as absent.

### Using Bootstrap directly

```go
boot := forgo.NewBootstrap(
	[]forgo.ResourceFactory{config.DB()},
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
	forgo.WithResources(config.DB(), ...),     // forgo.ResourceFactory values
	forgo.WithProviders(NewRepo, NewSvc, ...), // constructors, innermost first
	forgo.WithServers(config.HTTP, ...),       // forgo.ServerFactory values
)
if err := app.Run(); err != nil {
	os.Exit(1)
}
```

`Run` blocks until the application stops, and never exits, so `main` chooses the exit code after cleanup. It logs every failure with `err_severity`, and returns `nil` only when a signal stopped the app and every server and resource shut down cleanly. Otherwise it returns the failures joined:

| Failure | Returned when |
| --- | --- |
| No servers, or a nil server factory | Before anything is bootstrapped |
| A bootstrap error | Resources created so far are closed |
| A server's `New` or `Init` error, or a `New` that returns nil | Nothing serves; resources are closed |
| The first `Serve` error | After every server is terminated |
| A `Terminate` error, or a server killed after `TermTimeout` | Joined with the above |
| A resource `Close` error | Joined with the above |

An orchestrator then restarts a failed app even when it restarts only on failure, such as systemd's `Restart=on-failure`. Use `forgo.Env(key, default)` to read configuration from the environment.

### What `Run` does

1. Creates the app logger with `app_name` and `app_id` fields, and makes it the default logger (`logger.SetDefault`) until `Run` returns, so resources, providers and code that log without an operation's context use it too.
2. Builds the bootstrap: resources, then providers.
3. For each server factory, in order: calls `New(resolver)`, derives a server logger with `server_name` and `server_id`, and calls `Init(ctx)` with that logger in `ctx`.
4. Seals the bootstrap.
5. Starts every server's `Serve()` in its own goroutine.
6. Waits for SIGINT, SIGTERM, or the first `Serve` error. From then on, a signal is no longer caught: a second Ctrl+C terminates the process at once.
7. Terminates all servers **in parallel**. For each one:
   1. If `Term().TermSig` is set, it is signaled and the app sleeps for `TermSig.Wait`. This is the window for a readiness probe to fail and load balancers to drain.
   2. Calls `Terminate(ctx)`. `ctx` carries the server's logger and expires after `Term().TermTimeout`. A `TermTimeout` of zero or less means 30s, even for a server that does not sanitize its config.
   3. Waits for `Terminate` and `Serve` to return. If `Terminate` returns an error, or the deadline passes first, calls `Kill()` and stops waiting for the server.
8. Waits for every goroutine started with [`process.Go`](#background-goroutines).
9. Closes resources in reverse creation order.
10. Returns the failures, if any.

### Termination settings

Every built-in server config embeds `forgo.ServerConfig`:

```go
type ServerConfig struct {
	Name                   string              // the server's name; empty → its default, such as http
	Term                   TermConfig
	Middlewares            []MiddlewareFactory // server-wide middlewares
	DisableBaseMiddlewares bool                // drop the server's built-in chain
}

type TermConfig struct {
	TermSig     *TermSig      // optional readiness signal
	TermTimeout time.Duration // graceful termination budget; 0 or less → 30s
}

type TermSig struct {
	Wait time.Duration // pause after signaling, before Terminate
}

func (t *TermSig) Signaled() bool        // termination has started
func (t *TermSig) Done() <-chan struct{} // closed when termination starts
```

**Readiness pattern.** Give the HTTP server a `TermSig` and route a readiness probe to `http.HealthHandler`, which reads the `TermSig` of the server it is routed on:

```go
var HTTP = http.Config{
	ServerConfig: forgo.ServerConfig{
		Term: forgo.TermConfig{TermSig: &forgo.TermSig{Wait: 10 * time.Second}, TermTimeout: 20 * time.Second},
	},
	Routes: []http.RouteConfig{
		{Name: "ready", Method: "GET", Path: "/ready", Version: "v1", Handler: http.HealthHandler(),
			Args: map[any]any{http.ArgKeyRecorderDisabled: true}},
	},
}
```

On a gRPC server, `fgrpc.HealthService()` does the same through the gRPC health checking protocol. See [Health and reflection](#health-and-reflection).

On SIGTERM, `/v1/ready` starts answering 503 with `errCode: "terminating"`. The app waits 10s for traffic to drain, then shuts the HTTP server down gracefully within 20s. Combined with a Kubernetes rolling update, this makes deployments [zero-downtime](#zero-downtime-deployments-on-kubernetes).

### Zero-downtime deployments on Kubernetes

With the [readiness pattern](#termination-settings) above, a Kubernetes rolling update replaces every pod without failing a request. You don't need a `preStop` sleep hook, a sidecar or any shutdown code of your own. Deploy the app with a readiness probe on the `HealthHandler` route, a rolling update that never drops below the desired replica count, and a termination grace period that covers the drain:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: users-api
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1       # start a new pod first
      maxUnavailable: 0 # remove an old pod only after a new one is ready
  selector:
    matchLabels: {app: users-api}
  template:
    metadata:
      labels: {app: users-api}
    spec:
      terminationGracePeriodSeconds: 40 # TermSig.Wait (10s) + TermTimeout (20s) + 10s to close
      containers:
        - name: users-api
          image: registry.example.com/users-api:1.4.0
          ports: [{containerPort: 8080}]
          readinessProbe:
            httpGet: {path: /v1/ready, port: 8080}
            periodSeconds: 2
            failureThreshold: 1
```

**What happens during the rollout:**

1. Kubernetes starts a new pod. Its readiness probe passes only once the HTTP server listens. That happens after bootstrap has built every resource and provider and every server has initialized, so a pod that is still connecting to its database gets no traffic.
2. Once the new pod is ready, Kubernetes terminates an old one. With `maxUnavailable: 0`, capacity never drops below `replicas`.
3. Kubernetes removes the old pod from the Service endpoints and sends it SIGTERM at the same time. kube-proxy, ingress controllers and cloud load balancers apply the removal a few seconds later and keep routing requests to the pod until then.
4. The app signals the `TermSig`. The server keeps serving every request for `TermSig.Wait`, so requests routed late still succeed. Meanwhile `/v1/ready` answers 503, which tells everything that still checks the pod's readiness, such as a load balancer health-checking `/v1/ready`, to stop sending it traffic.
5. After `Wait`, the server shuts down gracefully: it stops accepting connections and lets in-flight requests finish within `TermTimeout`.
6. The app waits for [`process.Go`](#background-goroutines) goroutines, closes its resources and exits before the grace period ends.

**Settings that must agree:**

| Setting | Rule |
| --- | --- |
| `TermSig.Wait` | Longer than the readiness probe takes to fail (`periodSeconds × failureThreshold`) plus the time your proxies and load balancers take to drop the pod. 5 to 15 seconds is typical. |
| `TermTimeout` | Longer than your slowest request. Defaults to 30s. |
| `terminationGracePeriodSeconds` | More than `TermSig.Wait + TermTimeout` of the slowest server, plus the time to finish background goroutines and close resources. Kubernetes defaults to 30s, the same as `TermTimeout` alone, so set it explicitly or the pod is killed mid-drain. |
| `maxUnavailable` / `maxSurge` | `0` / `1` or more, so a pod is only removed after its replacement is ready. |

- Run the binary as the container's main process, for example with an exec-form `ENTRYPOINT ["/app"]`. With a shell-form entrypoint the shell is PID 1 and may not forward SIGTERM. The app then never drains and is killed when the grace period ends.
- If you add a liveness probe, point it at an `EmptyHandler` route, never at `HealthHandler`. It must keep passing while the pod drains.
- Only servers that receive traffic through a Service need a `TermSig`. The other servers of the app, such as a worker consumer or a cron server, start terminating at once, in parallel.

### Running several servers in one app

Pass several server factories to the same app. They share **one** bootstrap: the same resources and the same provider instances.

```go
forgo.NewApp(
	forgo.WithName("shop"),
	forgo.WithLogger(config.Logger),
	forgo.WithResources(config.DB(), config.Broker()),
	forgo.WithProviders(config.Providers...),
	forgo.WithServers(
		config.HTTP,      // public API on :8080
		config.GRPC,      // gRPC API on :9090
		config.AdminHTTP, // another http.Config on :8081
		config.Cron,      // scheduled jobs
		config.Consumer,  // message handlers
		config.Producer,  // outbox relay
	),
).Run()
```

All servers start together. If any `Serve` fails, every server is terminated and `Run` returns that error. On a signal, all of them terminate in parallel.

Each server is named by its kind (`http`, `grpc`, `cron`, `worker_consumer`, `worker_producer`): the `server_name` of its logs, the server of its connection metrics, and the name [`forgotest`](#testing) finds it by. Two servers of one kind, such as `config.HTTP` and `config.AdminHTTP` above, need names of their own, or their logs and metrics cannot be told apart and tests cannot address either. Set `ServerConfig.Name`:

```go
var AdminHTTP = http.Config{
	ServerConfig: forgo.ServerConfig{Name: "admin"},
	Addr:         ":8081",
}
```

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
	err := forgo.NewApp(
		forgo.WithName("shop-worker"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB(), config.Broker()),
		forgo.WithProviders(config.Adapters...),
		forgo.WithProviders(config.Services...),
		forgo.WithServers(config.Consumer, config.Producer),
	).Run()
	if err != nil {
		os.Exit(1)
	}
}

// cmd/scheduler/main.go
func main() {
	err := forgo.NewApp(
		forgo.WithName("shop-scheduler"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB()),
		forgo.WithProviders(config.Adapters...),
		forgo.WithProviders(config.Services...),
		forgo.WithServers(config.Cron),
	).Run()
	if err != nil {
		os.Exit(1)
	}
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

	err := forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithLogger(config.Logger),
		forgo.WithResources(config.DB(), config.Broker()),
		forgo.WithProviders(config.Providers...),
		forgo.WithServers(selected...),
	).Run()
	if err != nil {
		os.Exit(1)
	}
}
```

### App and CLI in one binary

The CLI reads its command name from `os.Args[1]`, so one binary can serve and run commands:

```go
func main() {
	run := newApp().Run
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		run = newCli().Run
	}
	if err := run(); err != nil {
		os.Exit(1)
	}
}
```

`./shop serve` runs the servers. `./shop migrate -a=up` runs a command.

### Reading the definition

`app.Definition()` returns what the options configured, as a `forgo.AppDefinition`:

| Field | Set by |
| --- | --- |
| `Name` | `WithName`, or `"default"` |
| `Logger` | `WithLogger` |
| `Resources` | `WithResources`, in order |
| `Providers` | `WithProviders`, in order |
| `Servers` | `WithServers`, in order |

It is for drivers other than `Run` that build the bootstrap and the servers themselves, such as [`forgotest`](#testing). The lists and `Logger.StaticFields` are copies, so a driver may change them, for example to replace a resource in a test, and the app stays as it is. The factories and providers in them are shared.

---

## Operations

Forgo abstracts every unit of work as an **operation**: an HTTP request, a gRPC call, a cron run, a consumed message, a CLI command, or anything a custom server does. Each transport translates its input into an `operation.Context` and runs a chain of `operation.Handler`s. The common execution logic is interpreted once, so the same middleware works everywhere.

### `operation.Context`

```go
type Context struct {
	context.Context
	ID    string               // operation id (HTTP: X-Request-ID, gRPC: x-request-id, or a generated UUIDv7)
	Name  string               // route name, job name, message name, command name
	Type  string               // "http_handler", "grpc_handler", "cron_job", "message_handler", "command", ...
	Args  *types.Map[any, any] // per-operation values, concurrency-safe
	State any                  // transport-specific state
}

func FromCtx(ctx context.Context) (*Context, bool)
```

- It is a `context.Context`. Handlers and services receive it as their `ctx`.
- `Value(key)` checks `Args` first, then the embedded context. Static values configured on a route or message handler (`Args: map[any]any{...}`) are in `Args` for each operation, so `ctx.Value(myKey)` returns them anywhere downstream. The server copies that map once at startup and every operation reads the copy in place. An operation that writes to its `Args` gets a private copy first, so writes never reach other operations. Changing the config map after startup has no effect.
- Use your own unexported key types for `Args`, as for `context.WithValue`. The framework's keys, such as `http.ArgKeyRecorderDisabled`, each have a type of their own package.
- Middlewares replace `Context` before they call the next handler, and `Tolerance` resets it before every attempt. A goroutine the operation starts may read the fields, but write only to `Args`.

| Transport | `Type` | `State` |
| --- | --- | --- |
| HTTP | `http.OperationTypeHTTPHandler` (`"http_handler"`) | `*http.State{Request *http.Request, Writer *http.ResponseRecorder}`, with `Terminating()` |
| gRPC | `grpc.OperationTypeGRPCHandler` (`"grpc_handler"`) | `*grpc.State{FullMethod string, Request, Response proto.Message}`, with `Metadata()`, `Peer()`, `SetHeader()`, `SendHeader()`, `SetTrailer()`, `WrapStream()`, `ClientStreams()`, `ServerStreams()`, `Terminating()` and `Code()` |
| Cron | `cron.OperationTypeCronJob` (`"cron_job"`) | `nil` |
| Worker consumer | `worker.OperationTypeMessageHandler` (`"message_handler"`) | `*worker.State{Delivery worker.IDelivery, Attempts int, Subscriber string}` |
| CLI | `cli.OperationTypeCommand` (`"command"`) | `*cli.State{Fields []string}` (the command line: `os.Args`, or the one given to `cli.WithArgs`) |

`FromCtx` returns the operation of a context: the context itself when it is the `*operation.Context` the transport passed in, or the innermost one it was derived from, as with `context.WithTimeout`. It returns `false` outside an operation:

```go
func (s *Service) Get(ctx context.Context, req GetReq) (GetResp, error) {
	op, ok := operation.FromCtx(ctx)
	if !ok {
		return GetResp{}, errors.New("service: not called from an operation")
	}
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
| `Error() string` | Returns `Message`, then `": "` and the cause when there is one, so wrapping errors and logs keep the whole chain. Transports send only `Code` and `Message`. Never send `Error()` to a client yourself, since the cause can hold internal details. |
| `Wrap(err) Error` | Returns a copy with `err` as the internal cause. The cause is logged, never sent to clients. `Wrap(nil)` removes it. |
| `Unwrap() error` | Returns the cause. |
| `Format(args...) Error` | Returns a copy with `Message` formatted by `fmt.Sprintf`. |
| `Is(target) bool` | Matches an `Error` target with the same non-empty `Code`, so `errors.Is` finds a declared error after `Format` and `Wrap`. |
| `As(target) bool` | Fills an `*Error` target, so a `*operation.Error` in a chain counts as an `operation.Error`. Return values anyway. |

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

`Format` and `Wrap` return modified copies, and `errors.Is` matches them by `Code`, also through `fmt.Errorf("…: %w", err)` and through other `Error`s that wrap them. Use `errors.As` to read the fields:

```go
if errors.Is(err, ErrOrderNotFound) { ... }

var oe operation.Error
if errors.As(err, &oe) && oe.Status == operation.StatusNotFound { ... }
```

An `Error` that wraps another keeps both in its text. A repository's `ErrDBUnavailable.Wrap(pgErr)` wrapped by a service's `ErrCreateOrder.Wrap(err)` logs `create order failed: database unavailable: <pgErr>`, with the code and severity of the outer error.

**Statuses** classify an error semantically. Each transport maps them to its own protocol codes:

| Status | Meaning | HTTP | gRPC |
| --- | --- | --- | --- |
| `StatusUnknown` | Nothing better is known | 500 | `Internal` |
| `StatusInvalidArgument` | Client sent invalid input | 400 | `InvalidArgument` |
| `StatusUnauthenticated` | Missing or invalid authentication | 401 | `Unauthenticated` |
| `StatusPermissionDenied` | Authenticated but not allowed | 403 | `PermissionDenied` |
| `StatusNotFound` | Resource does not exist | 404 | `NotFound` |
| `StatusConflict` | Conflicts with existing state (duplicate email…) | 409 | `AlreadyExists` |
| `StatusAborted` | Concurrent modification or aborted transaction; retry after refresh | 409 | `Aborted` |
| `StatusPreconditionFailed` | Business precondition not met (already paid…) | 412 | `FailedPrecondition` |
| `StatusRateLimited` | Limit exceeded | 429 | `ResourceExhausted` |
| `StatusCancelled` | Operation cancelled | 408 | `Canceled` |
| `StatusTimeout` | Operation timed out | 408 | `DeadlineExceeded` |
| `StatusUnavailable` | Dependency temporarily unavailable | 503 | `Unavailable` |
| `StatusInternal` | Unexpected server error | 500 | `Internal` |
| `StatusTooLarge` | Input larger than allowed, such as a request body | 413 | `ResourceExhausted` |
| `StatusUnimplemented` | Not implemented or not supported, such as an unknown gRPC method | 501 | `Unimplemented` |

`Status.String()` names a status in snake case, such as `not_found` or `rate_limited`, so that every metrics backend labels it alike. A value outside the table is named `unknown`, like `StatusUnknown`.

**Severities:** `SeverityLight` is a minor incident, logged as a warning. It is the zero value, so an `Error` declared without a `Severity` is light. `SeverityModerate` is significant and `SeverityCritical` needs handling as soon as possible; both are logged as errors. Each log entry carries a numeric `err_severity` field (0, 1, 2) for alert routing.

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

`Map` returns the first `operation.Error` in the error's chain unchanged, without running any mapper. Otherwise it tries each mapper in order. If none matches, it falls back to `operation.NewFallbackError(err)`: code `"internal"`, `StatusUnknown`, message `"an error occurred"`, `SeverityCritical`.

`operation.ErrorOf(err)` returns the first `operation.Error` in the chain, or `NewFallbackError(err)` when there is none, without running mappers. It is the error `Recorder` logs and `Observe` reports, and, unlike `errors.As` with a pointer to an `Error`, it does not allocate.

### Locks

```go
type Lock struct {
	Key string
	TTL time.Duration
}

type ILocker interface {
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) // false, nil: held by another
	WaitLock(ctx context.Context, key string, attempts int, delay, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key string) error // releases only its own lock
}

type LockCrafter func(ctx *Context) Lock
```

Cron jobs, CLI commands and message handlers take a `Lock` in their config. A lock applies only when **both** `Key` and `TTL` are set. Locking needs an `operation.ILocker` in the bootstrap, for example `locker.NewRedis` from [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#locker). Servers resolve it automatically when present. A `Lock` configured without a registered `ILocker` fails at startup.

The lock expires on its own after `TTL`, so a crashed process never holds it longer. Nothing renews it while the operation runs: set `TTL` above the longest run, including every retry of a cron job's `Tolerance`, or another process may start the operation while it still runs.

An `ILocker` returns `false` with a nil error when another holder has the lock, and an error only when it cannot tell. `Lock` tries once and must not wait for the lock. `WaitLock` tries up to `attempts` times (at least once), waiting `delay` between tries but not after the last one, and returns `ctx`'s error when `ctx` ends first. `Unlock` must release only a lock it acquired, and return an error for a lock that expired, since another process may hold it by then.

The built-in servers only use `Lock`: an operation whose lock is held is skipped or retried, never kept waiting. Resolve the `operation.ILocker` in your own code to lock around other work, and call `WaitLock` when that work should wait its turn:

```go
func (s *ReportService) Rebuild(ctx context.Context) error {
	ok, err := s.locker.WaitLock(ctx, "report:rebuild", 10, time.Second, time.Minute)
	if err != nil {
		return err
	}
	if !ok {
		return ErrReportBusy
	}
	defer s.locker.Unlock(context.WithoutCancel(ctx), "report:rebuild")
	return s.rebuild(ctx)
}
```

### Tolerance (retries)

```go
type Tolerance struct {
	Retry         bool
	RetryAttempts uint                                        // first attempt included; 0 = unlimited
	RetryDelay    time.Duration                               // fixed delay between attempts
	RetryTimeout  time.Duration                               // bounds the whole run, running attempts included; 0 = none
	RetryIf       func(err error) bool                        // nil = retry every error
	OnRetry       func(ctx *Context, attempt uint, err error) // before each retry; attempt counts from 1
	OnSuccess     func(ctx *Context)
	OnFailure     func(ctx *Context, err error) // with the error the operation returns
}

var DefaultTolerance = Tolerance{Retry: true, RetryAttempts: 5, RetryDelay: 5 * time.Second}
```

Used by cron jobs through `IJob.Tolerance()`, and by `middleware.Tolerance` anywhere else.

- The operation fails with the error of its **last** attempt, unchanged.
- With `RetryAttempts` 0, retries go on until an attempt succeeds, `RetryIf` refuses its error, or `RetryTimeout` or the operation's context ends. A cron job's context ends only when the app kills its server, so give cron jobs `RetryAttempts` or `RetryTimeout`. Without a `RetryDelay` as well, `Tolerance{Retry: true}` retries a failing operation in a tight loop.
- Each attempt runs on a context that ends `RetryTimeout` after the first attempt started, and no attempt starts after that. The wait between attempts also ends early when the operation's context ends.
- Every attempt reuses the same `*operation.Context`, so changes to `Args` and `State` carry over. Its `Context` is reset before each attempt, so values that middlewares inside the retries add do not pile up.

### Transactor

```go
type Transactor interface {
	Do(ctx context.Context, fn func(ctx context.Context) error) error
}
```

`Do` must run `fn` inside a transaction: commit when `fn` returns nil, roll back otherwise. It must call `fn` with a context derived from `ctx` that carries the transaction, so every repository or storage called with that context joins it. The worker consumer runs each message handler through it. The handler still receives an `*operation.Context`, now running on the transaction's context. For `database/sql`, [`sqltx.NewTransactor`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#sqltx) from forgo-adapter implements it, and `sqltx.From(ctx, db)` gives repositories the transaction to join.

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

Because every server runs operations, the **same middleware** can be attached to HTTP routes, gRPC services, cron servers, message handlers and CLI commands:

```go
audit := forgo.MiddlewareAs(NewAuditMiddleware)

http.Config{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{audit}}, ...}
fgrpc.Config{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{audit}}, ...}
cron.Config{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{audit}}, ...}
worker.ConsumerConfig{ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{audit}}, ...}
cli.Config{Middlewares: []forgo.MiddlewareFactory{audit}, ...}
```

When a middleware needs transport details, switch on `State`:

```go
switch st := ctx.State.(type) {
case *http.State:
	tenant = st.Request.Header.Get("X-Tenant")
case *fgrpc.State:
	if v := st.Metadata().Get("x-tenant"); len(v) > 0 {
		tenant = v[0]
	}
case *worker.State:
	tenant, _ = st.Delivery.Message().Headers.Get("x-tenant")
}
```

On HTTP routes, attach a middleware that reads request parameters with `http.MiddlewareAs`, and one that authenticates requests, rejects them or sets headers with `http.WithContract`, so the generated API documentation lists what it reads and answers. See [Middlewares that read request parameters](#middlewares-that-read-request-parameters) and [Middleware contracts](#middleware-contracts).

### Order

Every server builds the chain as **base middlewares → server middlewares → narrower scopes → handler**. Base middlewares are skipped when `DisableBaseMiddlewares` is true. Recovering panics is then up to your chain: put `middleware.Recovery` in it, outside the middlewares and handlers whose panics it should catch.

| Server | Base chain (outer → inner) | Then |
| --- | --- | --- |
| HTTP | `ID` → `Logger` → `Recorder` (request/response fields) → `Observe` → `ErrorResponder` (writes unwritten errors, panics included) → `Recovery` | server → route → handler |
| gRPC | `ID` → `Logger` → `Recorder` (metadata, request/response messages, stream message counts) → `Observe` → stream message logger (with `LogStreamMessages` only) → `Responder` (sends the response, or chooses the status, panics included) → `Recovery` | server → service → method → convert/validate/handler |
| Cron | `ID` → `Logger` → `Locker` (job lock) → `Recorder` → `Observe` → `Tolerance` (job tolerance) → `Recovery` | server → job → `Run` |
| Worker consumer | `ID` → `Logger` (+`msg_id`) → `Locker` (per message) → `Recorder` (headers, payload) → `Observe` → `Recovery` (retry/reject) → idempotency guard (inbox) | server → subscriber → handler → bind/transaction/handler |
| CLI | `ID` → `Logger` → `Locker` (command lock) → `Recorder` → `Observe` → `Recovery` | cli → command → parse/validate/run |

`Observe` is in a base chain only when the bootstrap provides an `operation.IObserver`. See [Metrics](#metrics).

The HTTP server also ships opt-in `CORS`, `RateLimit` and `JWTAuth` middlewares. See [Built-in HTTP middlewares](#built-in-http-middlewares). They read `*http.State`, so they serve HTTP only.

### Built-in middlewares (`github.com/efegreat/forgo/middleware`)

| Middleware | Behavior |
| --- | --- |
| `ID()` | Keeps the ID the transport set, such as the HTTP `X-Request-ID` header or the gRPC `x-request-id` metadata, when it is at most 128 bytes of visible ASCII. Otherwise, and when it is empty, sets a new UUIDv7. |
| `Logger(adder logger.FieldAdder)` | Derives a logger with `op_type`, `op_name`, `op_id` plus fields from `adder`, and stores it in the context. Runs after `ID`. |
| `Recorder(disabled, before, after)` | Logs `operation started` and then `operation completed` (with `elapsed`) or `operation failed` (with `err_code`, `err_severity`, `elapsed`, the error, and a warn/error level by severity). The code and severity are those of the first `operation.Error` in the error's chain. The logged error is the operation's error itself, whose text holds the internal cause of every `operation.Error` in it. `before`/`after` add fields, and run only when their event is logged, so a level above info skips their work; `disabled(ctx)` skips logging. |
| `Observe(o operation.IObserver)` | Calls `o.Start` before the operation and `o.End` after it, with its elapsed time, its error and `operation.ErrorOf` of that error. A nil `o` adds nothing to the chain. It recovers no panic: put it outside `Recovery`, which turns a panic into the error it reports. See [Metrics](#metrics). |
| `Recovery(on func(*operation.Context, *middleware.PanicError) error)` | Turns a panic into a `*middleware.PanicError`, which holds the panic `Value` and the `Stack` and unwraps to `Value` when it is an error. With `on == nil` the operation fails with it; otherwise with what `on` returns. |
| `Locker(l, crafter, onConflict)` | Acquires the lock built by `crafter` before running and releases it after, even after a panic. If another holder has it, logs at info and returns what `onConflict` returns (or nil, skipping the run). If the lock cannot be acquired, logs the failure and returns it. A nil `crafter`, or a lock without key or TTL, runs without a lock. |
| `Tolerance(conf operation.Tolerance)` | Retries the rest of the chain according to `conf`. See [Tolerance](#tolerance-retries). |
| `Guard(check, reject)` | Runs `check`. If it allows, the operation proceeds. Otherwise it fails with what `reject` returns, or `middleware.ErrRejected` (`rejected`, permission denied) without `reject`. A `reject` that returns nil ends the operation successfully without running it. A nil `check` allows everything. |

---

## HTTP server

Built on the standard library `net/http` with Go's pattern-based `ServeMux`.

### Configuration

```go
var HTTP = http.Config{
	ServerConfig: forgo.ServerConfig{
		Term:        forgo.TermConfig{TermTimeout: 20 * time.Second},
		Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewAuditMiddleware)},
	},
	Addr:       ":8080",
	Validators: map[string]validator.Func{"slug": isSlug},
	Routes:     routes,
}
```

| Field | Default | Description |
| --- | --- | --- |
| `ServerConfig` | | `Name`, termination, server-wide middlewares, `DisableBaseMiddlewares`. |
| `Network` | `"tcp"` | Listener network. |
| `Addr` | `":8080"` | Listen address. |
| `ReadTimeout` | `60s` | `http.Server.ReadTimeout`: reading a whole request, body included. |
| `ReadHeaderTimeout` | `0` | `http.Server.ReadHeaderTimeout`. `0` means `ReadTimeout`. |
| `WriteTimeout` | `10s` | `http.Server.WriteTimeout`: from the end of the request headers to the end of the response, so it also bounds handlers. |
| `IdleTimeout` | `60s` | `http.Server.IdleTimeout`. |
| `MaxHeaderBytes` | `0` | `http.Server.MaxHeaderBytes`. `0` means 1 MiB. |
| `MaxBodyBytes` | 1 MiB | Request body limit. Reading past it fails, and the request is answered `413` with `errCode: "body_too_large"`. `RouteConfig.MaxBodyBytes` overrides it. Negative means no limit. |
| `TLSConfig` | | Serves HTTPS when set. Certificates come from `TLSConfig.Certificates`, `GetCertificate` or `GetConfigForClient`. |
| `DisableGeneralOptionsHandler`, `TLSNextProto`, `ConnState`, `ConnContext`, `HTTP2`, `Protocols` | | Passed through to `http.Server`. |
| `ErrorLog` | zerolog adapter | `net/http`'s internal error log, written as `http server error` entries. |
| `Observer` | | A `transport.IConnObserver` that is told about accept errors, for metrics or alerts. One that also implements `transport.IConnIOObserver` is told about read and write errors as well, and one that implements `transport.IConnStateObserver` about the state of every connection, before `ConnState`. Without it, an `operation.IObserver` of the bootstrap that implements `transport.IConnObserverFactory` gives the server its observer. See [`transport`](#transport). |
| `Validators` | | Custom validation tags for the default binder. An empty name or a nil function fails `New`. |
| `Binder` | bingo binder | Request binder (`http.IBinder`). |
| `Serializer` | JSON envelope | Response serializer factory (`http.SerializerFactory`). |
| `ErrorMapper` | `MapBody`, `MapBinding`, `MapValidation` | Maps the errors of binding, handlers and middlewares to `operation.Error`. See [Responses](#responses). |
| `SensitiveFields` | | Names whose values are masked as `[*]` in logs. See [Masking sensitive fields](#masking-sensitive-fields). |
| `Routes` | | Route list. |

### Routes

```go
type RouteConfig struct {
	Name          string                    // operation name; OpenAPI operationId
	Description   string                    // OpenAPI description
	Method        string                    // "GET", "POST", …; empty matches every method
	Path          string                    // ServeMux path pattern, e.g. "/users/{id}"
	Version       string                    // path prefix, e.g. "v1"
	Middlewares   []forgo.MiddlewareFactory // route middlewares
	Handler       http.HandlerFactory       // HandlerFor / HandlerAs / EmptyHandler / HealthHandler / live config handlers
	Args          map[any]any               // static values in ctx.Args of every request
	MaxBodyBytes  int64                     // overrides Config.MaxBodyBytes, such as for uploads; negative means no limit
	DisableAPIDoc bool                      // leave the route out of OpenAPI documents; it is served as usual
}
```

A route is registered as `"<Method> /<Version><Path>"`. Method `GET`, version `v1` and path `/users/{id}` become `GET /v1/users/{id}`. With an empty `Version`, the route is served at `<Path>` alone. `RouteConfig.FullPath()` returns the resulting path. Paths use standard `ServeMux` syntax: `{name}` wildcards, `{name...}` for the remainder, `{$}` for an exact match.

`New` fails, naming the route, when a route has no `Handler`, a lower-case standard method such as `"get"` (methods are case-sensitive, so it would never match), or a handler or middlewares that cannot be resolved:

```text
http: route "get_user" (GET /v1/users/{id}): missing dependency *user.Service
```

An invalid pattern, or one that conflicts with another route's, makes `ServeMux` panic at startup.

A request that no route matches, including a known path with a different method, gets a JSON `404` with `errCode: "route_not_found"`. It runs as the `not_found` operation through the base and server middlewares. A route with an empty `Method` and the path `/` replaces this fallback.

### Handlers

All handlers share one signature:

```go
type Handler[Req, Resp any] func(ctx context.Context, req Req) (Resp, error)
```

Four ways to declare one:

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

// 3. No request, empty success response. Ideal for liveness checks.
Handler: http.EmptyHandler(),

// 4. Like EmptyHandler, but 503 once the server's TermSig is signaled. Ideal for readiness probes.
//    See the readiness pattern in Termination settings.
Handler: http.HealthHandler(),
```

Each request goes through: bind and validate `Req` → call the handler → serialize `Resp` or the mapped error.

**Writing the response yourself.** For a redirect, a file, a stream or anything else than the JSON envelope, write through the request's `*http.State`, which handlers reach with `operation.FromCtx`. The server answers only while nothing is written, so it keeps what the handler wrote, even when the handler then returns an error. The error is still logged.

```go
func (s *Service) Download(ctx context.Context, req DownloadReq) (struct{}, error) {
	doc, err := s.docs.Get(ctx, req.ID)
	if err != nil {
		return struct{}{}, err // nothing written yet: answered as usual
	}

	op, _ := operation.FromCtx(ctx)
	st := op.State.(*http.State)
	nethttp.ServeContent(st.Writer, st.Request, doc.Name, doc.UpdatedAt, bytes.NewReader(doc.Content))
	return struct{}{}, nil
}
```

`st.Writer` is an `*http.ResponseRecorder`, which writes through to the client and records the status and the head of the body for the request log. Headers set without writing, such as `Cache-Control` or `Location`, stay on the envelope the server then writes. `http.NewResponseController(st.Writer)` reaches `Flush` and the other optional interfaces of the writer below. Such a route is still documented with the envelope by the `openapi` command: hide it with `DisableAPIDoc` or describe it with a `WithDocument` hook.

**Long-lived responses and WebSockets.** `st.Terminating()` is closed when the server starts terminating. A response that never ends by itself, such as server-sent events, returns when it closes, since `Terminate` waits for it:

```go
func (s *Service) Events(ctx context.Context, _ struct{}) (struct{}, error) {
	op, _ := operation.FromCtx(ctx)
	st := op.State.(*http.State)
	st.Writer.Header().Set("Content-Type", "text/event-stream")
	rc := nethttp.NewResponseController(st.Writer)
	for {
		select {
		case e := <-s.events:
			if _, err := fmt.Fprintf(st.Writer, "data: %s\n\n", e); err != nil {
				return struct{}{}, err // the client is gone
			}
			if err := rc.Flush(); err != nil {
				return struct{}{}, err
			}
		case <-st.Terminating():
			return struct{}{}, nil // let the server stop gracefully
		case <-ctx.Done():
			return struct{}{}, ctx.Err()
		}
	}
}
```

A WebSocket library upgrades the connection from `st.Writer` and `st.Request`: `ResponseRecorder` implements `http.Hijacker`, and `http.NewResponseController` reaches its `Hijack`. Once a handler hijacked its connection, it answers on it, so the server writes no response and the request log has status `0`, unless the library sent `101` through the writer before. `Terminate` waits for the handler to return, so it watches `st.Terminating()` like the stream above, and `Kill` closes the connection. Hijacking works on HTTP/1 only, as in net/http.

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

- `reqId` is the operation ID: the incoming `X-Request-ID` header when it is at most 128 bytes of visible ASCII, or a generated UUIDv7.
- `EmptyHandler` and `HealthHandler` respond without a `result` field.
- Handler errors go through the `ErrorMapper`. The `operation.Error` status picks the HTTP status code, and `Code`/`Message` become `errCode`/`errMsg`. The internal cause is only logged.
- An error that maps to nothing becomes `500` with `errCode: "internal"`, `errMsg: "an error occurred"`. So does a result that cannot be encoded, such as a `NaN`.
- A panic in the handler or any non-base middleware is recovered and answered the same way, unless a response was already written, which is then kept. The logged error is the `*middleware.PanicError`, with its stack.
- The request log has the handler's error itself, with what is wrapped around its `operation.Error`: `fmt.Errorf("charge order 42: %w", ErrDeclined)` answers `ErrDeclined` and logs `charge order 42: card declined`.

Default error mappers, all exported so you can reuse them:

| Mapper | Matches | `errCode` | Status | `errMsg` |
| --- | --- | --- | --- | --- |
| `http.MapBody` | `*http.MaxBytesError`: a body over `MaxBodyBytes` | `body_too_large` | 413 | `request body larger than <n> bytes` |
| | `bingo.BodyError` of a truncated body | `eof` | 400 | `request body EOF` |
| | `bingo.BodyError` of a `*json.SyntaxError` | `json_syntax` | 400 | `invalid json syntax` |
| | `bingo.BodyError` of a `*json.UnmarshalTypeError` | `json_type` | 400 | `type error, field: …, required type: …` |
| | any other `bingo.BodyError`, such as two JSON documents or a field bound from another source | `invalid_body` | 400 | `invalid request body` |
| `http.MapBinding` | `bingo.FieldError` | `binding` | 400 | `invalid param for <source>: <name>` |
| `http.MapValidation` | `validator.ValidationErrors` | `validation` | 400 | `field: …, rule: …; …` |

`MapBody` matches only errors of the request body. A handler error that wraps `io.EOF` or a JSON error, such as from a call to another service, stays a `500`.

Setting `ErrorMapper` **replaces** the defaults. Include them if you still want them:

```go
ErrorMapper: operation.NewErrorMapper(
	http.MapBody, http.MapBinding, http.MapValidation,
	mapSQLErrors,
),
```

A mapper that returns an `operation.Error` without a cause gets the error it mapped as its cause, so the log keeps it.

**Custom serializer.** Implement `transport.ISerializer` and provide a factory:

```go
type SerializerFactory func(id string, w http.ResponseWriter) transport.ISerializer

type ISerializer interface {
	Success(result any) error // write the success response; return nil or the write/serialization failure
	Error(err error) error    // write the error response; return the error the operation fails with
}
```

The factory is called each time the server answers, only while nothing is written yet, and the transport calls `Success` or `Error` once. A serializer that is called again must not write again. The default serializer returns `http.ErrAlreadyWritten` in that case.

`Error` receives the mapped `operation.Error`. Its `Status`, `Code` and `Message` make up the response. `Error` returns `nil` once the response is written: the operation then fails with the error the server answered, which `Recorder` logs whole. When the write fails, it returns `err` wrapped with that failure (`operation.Error.Wrap`), so that the log keeps the code and severity. Returning `err` itself once the response is written also works, but the log then loses what was wrapped around it.

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

The error is written only if nothing has been written yet. With `DisableBaseMiddlewares`, the transport writes it after the chain returns. To add `ErrorResponder` to your own chain, use `http.ErrorResponder(serializer, errorMapper)`. To have the generated API documents list the response, declare it in a [middleware contract](#middleware-contracts).

### Middlewares that read request parameters

A middleware that reads request parameters (headers, query or path wildcards) is attached with `http.MiddlewareAs[Params](constructor)` instead of `forgo.MiddlewareAs(constructor)`. The [`openapi`](#openapi-api-documents) command then documents those parameters on every route the middleware runs on. `Params` is a struct tagged like a handler request, with `header`, `query`, `uri` and `validate` tags:

```go
type TenantParams struct {
	TenantID string `header:"X-Tenant-ID" validate:"required,uuid"`
}

func NewTenantMiddleware(tenants *tenant.Store) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			id := ctx.State.(*http.State).Request.Header.Get("X-Tenant-ID")
			// ...
			return next(ctx)
		}
	}
}

// Declared once, next to the middleware, and attached to the server or to routes.
var TenantMiddleware = http.MiddlewareAs[TenantParams](NewTenantMiddleware)
```

`Params` only drives the generated API documentation. The server never binds or validates it, and the middleware still reads the request itself. Keep `Params` in line with what the middleware reads. Its `validate` tags only shape the documented schema. On a server, `http.MiddlewareAs` behaves exactly like `forgo.MiddlewareAs`. Middlewares without parameters keep using `forgo.MiddlewareAs`, or `http.WithContract` when they answer something worth documenting.

### Middleware contracts

`http.WithContract(contract, factory)` declares what a middleware of your own reads and answers. The [`openapi`](#openapi-api-documents) command then documents it on every route the middleware runs on, as it does for the [built-in middlewares](#built-in-http-middlewares) `JWTAuth` and `RateLimit`, which declare theirs the same way. Use it for a middleware that authenticates requests, rejects them or sets response headers. Every field of the `http.MiddlewareContract` is optional:

| Field | What it documents |
| --- | --- |
| `Params` | The request parameters the middleware reads: a struct type tagged like the `Params` of [`http.MiddlewareAs`](#middlewares-that-read-request-parameters), such as `reflect.TypeFor[TenantParams]()`. |
| `Auth` | How it authenticates requests: the `Credentials` it accepts, any one of which is enough, and `Optional` when it lets requests without one through. |
| `Headers` | The headers it sets on every response once it ran, including its own rejections and those of the middlewares after it. |
| `Rejections` | The responses it answers itself: the error it fails a request with (`Err`), and the headers it adds to that response (`Headers`). |

An API key middleware that also accepts the key in the query, and answers 401 without a known key:

```go
var ErrUnknownKey = operation.Error{
	Code: "unknown_api_key", Status: operation.StatusUnauthenticated, Message: "unknown API key",
}

func NewAPIKeyAuth(keys *auth.KeyStore) operation.Middleware {
	return func(next operation.Handler) operation.Handler {
		return func(ctx *operation.Context) error {
			r := ctx.State.(*http.State).Request
			key := cmp.Or(r.Header.Get("X-API-Key"), r.URL.Query().Get("api_key"))
			if !keys.Valid(key) {
				return ErrUnknownKey
			}
			return next(ctx)
		}
	}
}

// Declared once, next to the middleware, and attached to the server or to routes.
var APIKeyAuth = http.WithContract(http.MiddlewareContract{
	Auth: &http.AuthContract{Credentials: []http.Credential{
		{Kind: http.CredentialHeader, Name: "X-API-Key"},
		{Kind: http.CredentialQuery, Name: "api_key"},
	}},
	Rejections: []http.Rejection{{Err: ErrUnknownKey}},
}, forgo.MiddlewareAs(NewAPIKeyAuth))
```

Each credential becomes a security scheme of the routes the middleware runs on:

| `Kind` | The request sends | Security scheme |
| --- | --- | --- |
| `http.CredentialBearer` | `Authorization: Bearer <token>`. `BearerFormat` names its format, such as `JWT`. | `bearerAuth` (`http`, `bearer`) |
| `http.CredentialBasic` | `Authorization: Basic <credentials>` | `basicAuth` (`http`, `basic`) |
| `http.CredentialHeader` | The header `Name` | An `apiKey` in the header, named after it |
| `http.CredentialQuery` | The query parameter `Name` | An `apiKey` in the query, named after it |
| `http.CredentialCookie` | The cookie `Name` | An `apiKey` in the cookie, named after it |

- **The contract only documents.** The server never checks it, so keep it in line with what the middleware reads and answers.
- **Rejections are answered like any error**, through the server's `ErrorMapper` and `Serializer`. The generator probes both, so the documented status and body are the ones clients get. Rejections with the same status are documented as one response. Leave the middleware's own failures, such as a key store it cannot reach, out of `Rejections`: they belong to the `default` response.
- **A contract replaces the factory's own.** Set `Params` in the contract rather than wrapping an `http.MiddlewareAs`.
- **On a server, `http.WithContract` is the factory it wraps.** The generator reads the contract without calling the factory, so it never resolves the middleware's dependencies. The gRPC server ignores contracts, so a middleware that serves both transports can carry one.
- **Some contracts cannot be documented, and fail generation:** an `Auth` without credentials, a credential without a `Kind`, a header, query or cookie credential without a `Name`, a header without a name, a rejection without an error, and two different schemes with one name, such as a cookie credential named `bearerAuth`.

`http.MiddlewareContractOf(factory)` returns the `http.MiddlewareContract` of a factory, for tools of your own: the contract of `http.WithContract`, the `Params` of `http.MiddlewareAs`, and the contracts of `RateLimit`, `RateLimitWith` and `JWTAuth`. It never constructs a middleware or resolves a dependency of the application, and returns false for any other factory.

### Built-in HTTP middlewares

`github.com/efegreat/forgo/server/http` ships three opt-in middlewares: `CORS`, `RateLimit` and `JWTAuth`. They are not in the base chain. `RateLimit` and `JWTAuth` are thin layers over two transport-neutral packages, `github.com/efegreat/forgo/ratelimit` and `github.com/efegreat/forgo/jwtauth`, which the gRPC server's middlewares of the same names share (see [gRPC authentication and rate limiting](#authentication-and-rate-limiting)). Each one takes its config and returns a `forgo.MiddlewareFactory`, which goes straight into a `Middlewares` list. The config is sanitized, validated and compiled once, when you call `http.CORS(...)`, `http.RateLimit(...)` or `http.JWTAuth(...)`, and every route the factory is attached to shares the result. An invalid config fails the server's `New`, and dependencies such as a shared rate limiter are resolved from bootstrap:

```go
var HTTP = http.Config{
	ServerConfig: forgo.ServerConfig{
		Middlewares: []forgo.MiddlewareFactory{
			http.CORS(http.CORSConfig{AllowOrigins: []string{"https://app.example.com"}}),
			http.RateLimit(http.RateLimitConfig{Limit: 100, Period: time.Minute}),
		},
	},
	Routes: []http.RouteConfig{
		{
			Name: "me", Method: "GET", Path: "/me", Version: "v1",
			Middlewares: []forgo.MiddlewareFactory{
				http.JWTAuth(http.JWTConfig{
					Keys:     &jwtauth.JWKS{URL: "https://idp.example.com/.well-known/jwks.json"},
					Issuer:   "https://idp.example.com/",
					Audience: []string{"shop-api"},
				}),
			},
			Handler: http.HandlerFor((*user.Service).Me),
		},
	},
}
```

Order them `CORS` → `RateLimit` → `JWTAuth`. CORS then answers preflight requests before they are counted or rejected, and rejections from the later middlewares still carry the CORS headers.

The [`openapi` command](#openapi-api-documents) documents what `RateLimit`, `RateLimitWith` and `JWTAuth` read and answer, on every route they run on: the security schemes of the token, the `401` and `429` responses, and the `RateLimit-*`, `Retry-After` and `WWW-Authenticate` headers.

#### CORS

```go
func CORS(cfg CORSConfig) forgo.MiddlewareFactory
```

The zero `CORSConfig` allows every origin without credentials.

| Field | Default | Description |
| --- | --- | --- |
| `AllowOrigins` | `"*"` | Allowed origins, such as `"https://app.example.com"`. `"*"` allows every origin. One `*` inside an origin matches any non-empty text: `"https://*.example.com"`, `"http://localhost:*"`. Matching ignores case. |
| `AllowOriginFunc` | | `func(origin string) bool` for origins only known at runtime. It is asked about origins `AllowOrigins` does not allow. When it is set, `AllowOrigins` has no default. |
| `AllowMethods` | `GET, HEAD, PUT, PATCH, POST, DELETE` | `Access-Control-Allow-Methods`. |
| `AllowHeaders` | the requested headers | `Access-Control-Allow-Headers`. By default, whatever headers a preflight asks for are allowed. |
| `ExposeHeaders` | `X-Request-ID`, `Retry-After`, `RateLimit-*` | `Access-Control-Expose-Headers`. |
| `AllowCredentials` | `false` | Sends `Access-Control-Allow-Credentials: true`. Combined with `"*"`, startup fails. |
| `MaxAge` | `10m` | `Access-Control-Max-Age`. A negative value sends `0`, which disables preflight caching. |

- The middleware answers a preflight request (`OPTIONS` with `Origin` and `Access-Control-Request-Method`) itself, with `204`. A disallowed origin gets no CORS headers, so the browser fails the request.
- Other requests run normally. From an allowed origin they get `Access-Control-Allow-Origin`: `*` when every origin is allowed, otherwise the origin itself. Error responses carry it too, so scripts can read them.
- Unless every origin is allowed, responses carry `Vary: Origin`, so shared caches never serve one origin's response to another.
- **Attach CORS to the server middlewares.** A preflight uses `OPTIONS`, so it matches no route declared with another method. It is served by the `not_found` fallback, which runs only the base and server middlewares.
- Origins are validated at startup: `scheme://host[:port]`, no path, at most one `*` after the scheme.

#### Rate limiting

```go
func RateLimit(cfg RateLimitConfig) forgo.MiddlewareFactory
func RateLimitWith(cfg RateLimitConfig) forgo.MiddlewareFactory // counts in the bootstrap's ratelimit.ILimiter
```

The zero `RateLimitConfig` admits 60 requests per minute per client IP.

| Field | Default | Description |
| --- | --- | --- |
| `Limit` | `60` | Requests admitted per `Period`. |
| `Period` | `1m` | The time `Limit` applies to. |
| `Burst` | `Limit` | How many requests may arrive at once. Capacity refills continuously, one request every `Period/Limit`. |
| `Key` | client IP | `func(*operation.Context) (string, error)` that returns the key a request is counted under. An empty key exempts the request, and an error fails it. The default key counts IPv6 clients by their /64 network. |
| `TrustedProxies` | | `[]netip.Prefix` of your reverse proxies. The default key reads `X-Forwarded-For` only when the peer is one of them. |
| `Prefix` | | Prepended to every key. Give each `RateLimitWith` middleware that shares a limiter its own prefix. |
| `FailOpen` | `false` | What happens when the limiter fails. `false` fails the request with `rate_limit_unavailable` (503). `true` admits it and logs the error. |
| `DisableHeaders` | `false` | Omits the `RateLimit-*` response headers. |

Responses carry `RateLimit-Limit` (the burst), `RateLimit-Remaining` and `RateLimit-Reset` (seconds until the full burst is available again). A rejected request gets `429` with `errCode: "rate_limited"` and `Retry-After` in seconds.

`RateLimit` counts in its own in-process `ratelimit.Memory`. Attached to the server, the limit covers all routes together, unmatched requests included. Attached to a route, it covers that route alone. Counts are kept per process.

To enforce a limit across instances, provide a `ratelimit.ILimiter` backed by a shared store and use `RateLimitWith`. The factory resolves the limiter from bootstrap, and the server's `New` fails without one. The gRPC server's `RateLimitWith` can share it, with another `Prefix`. [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#ratelimit) ships one on Redis, with the same algorithm as `ratelimit.Memory`:

```go
// package github.com/efegreat/forgo/ratelimit
type ILimiter interface {
	Allow(ctx context.Context, key string, rate Rate) (Result, error)
}

forgo.WithResources(redisConf)               // redisdb.Config from forgo-adapter
forgo.WithProviders(redisratelimit.NewRedis) // forgo-adapter's provider/ratelimit, imported as redisratelimit

http.RateLimitWith(http.RateLimitConfig{Limit: 5, Period: time.Minute, Prefix: "login:"})
```

`ratelimit.Memory` implements the generic cell rate algorithm (GCRA) and stores one timestamp per active key. Keys whose budget has fully recovered are swept, and at most `maxKeys` keys are tracked. `ratelimit.NewMemory(maxKeys)` creates one you can share through a provider, where 0 means 1 << 20 keys.

`http.ClientIP(r, trustedProxies)` resolves the client address the same way the default key does. Use it to build your own keys. In this example, `JWTAuth` runs before `RateLimit` so the claims are available:

```go
Key: func(ctx *operation.Context) (string, error) {
	if c, ok := jwtauth.ClaimsFromCtx(ctx); ok {
		return "user:" + c.Subject, nil
	}
	return "ip:" + http.ClientIP(ctx.State.(*http.State).Request, proxies).String(), nil
},
```

#### JWT authentication

```go
func JWTAuth(cfg JWTConfig) forgo.MiddlewareFactory
```

| Field | Default | Description |
| --- | --- | --- |
| `Keys` | required | `jwtauth.Keys` for keys known up front, `*jwtauth.JWKS` for a key set URL, or any `jwtauth.IKeySet`. |
| `Issuer` | | When set, the `iss` claim must equal it. |
| `Audience` | | When set, the `aud` claim must contain one of these values. |
| `Leeway` | `0` | Clock skew tolerated when `exp` and `nbf` are checked. |
| `Cookie` | | Name of a cookie the token is read from when the request has no bearer token. |
| `Optional` | `false` | Requests without a token proceed, without claims. A token that is sent must still be valid. |

The token is read from `Authorization: Bearer <token>`, then from `Cookie` if it is configured. A `jwtauth.Authenticator` checks it: the token is parsed, its signature verified and its claims validated by [golang-jwt/jwt/v5](https://github.com/golang-jwt/jwt), and forgo supplies the keys and the rules:

- **Algorithms:** `HS256`, `HS384`, `HS512`, `RS256`, `RS384`, `RS512`, `PS256`, `PS384`, `PS512`, `ES256`, `ES384`, `ES512` and `EdDSA`. `none` is never accepted.
- **Keys bound to algorithms:** each algorithm accepts only keys of its own type and strength: HMAC secrets at least as long as the hash, RSA keys of at least 2048 bits, ECDSA keys on the algorithm's curve. This defeats algorithm confusion: an RSA public key is never used as an HMAC secret. `jwtauth.Key.Algorithm` pins a key to a single algorithm. Keys from a custom `jwtauth.IKeySet` are held to the same rules.
- **Claims:** `exp` is required. `exp`, `nbf`, `iss` and `aud` are validated. Tokens with a `crit` header are rejected, and base64 must be canonical.
- **Key selection:** when a token's `kid` names a key, only keys with that ID are tried. Otherwise, the keys without an ID are tried.

| Request | Status | `errCode` | `WWW-Authenticate` |
| --- | --- | --- | --- |
| No token | 401 | `unauthenticated` | `Bearer` |
| Expired token | 401 | `token_expired` | `Bearer error="invalid_token"` |
| Any other invalid token | 401 | `invalid_token` | `Bearer error="invalid_token"` |
| Keys cannot be fetched | 503 | `auth_unavailable` | |

**Static keys.** Invalid keys fail at startup.

```go
type Key struct { // package github.com/efegreat/forgo/jwtauth
	ID        string // matched against kid
	Algorithm string // optional pin, such as "RS256"
	Key       any    // []byte (≥ 32 bytes; HS384 ≥ 48, HS512 ≥ 64), *rsa.PublicKey (≥ 2048 bits), *ecdsa.PublicKey (P-256, P-384, P-521) or ed25519.PublicKey
}

Keys: jwtauth.Keys{{ID: "2026-10", Algorithm: "HS256", Key: []byte(forgo.Env("JWT_SECRET", ""))}},
```

**Key sets.** `*jwtauth.JWKS` fetches an identity provider's JSON Web Key Set.

| Field | Default | Description |
| --- | --- | --- |
| `URL` | required | The provider's `jwks_uri`. |
| `Client` | `http.DefaultClient` | Each fetch is limited to 10 seconds and 1 MiB. |
| `RefreshInterval` | `1h` | Keys older than this are fetched again. |
| `MinRefreshInterval` | `1m` | Minimum time between two fetches. |

Keys are fetched on first use and cached. They are fetched again once they are stale, or when a token names an unknown `kid`, which is how key rotations are picked up. Fetches happen at most once per `MinRefreshInterval`, so tokens with made-up key IDs cannot flood the provider. Requests never wait for a fetch they do not need: while one request refreshes stale keys, the others keep using them. While the provider is down, the keys fetched last stay in use. Only RSA, EC and Ed25519 keys whose `use` is empty or `sig` are loaded.

**Claims.** Handlers and later middlewares read the verified claims with `jwtauth.ClaimsFromCtx`, on either server:

```go
func (s *Service) Me(ctx context.Context, _ MeReq) (MeResp, error) {
	claims, _ := jwtauth.ClaimsFromCtx(ctx)

	var custom struct {
		Scope string `json:"scope"`
	}
	if err := claims.Decode(&custom); err != nil {
		return MeResp{}, err
	}
	return s.get(ctx, claims.Subject)
}
```

`jwtauth.Claims` has `Issuer`, `Subject`, `Audience`, `ExpiresAt`, `NotBefore`, `IssuedAt` and `ID`. Time claims are whole seconds, and those a token does not have are zero. `Decode(v)` unmarshals the whole payload, for custom claims.

**Authorization.** To check scopes or roles, add a `middleware.Guard` after `JWTAuth`:

```go
func RequireScope(scope string) func() operation.Middleware {
	return func() operation.Middleware {
		return middleware.Guard(
			func(ctx *operation.Context) (bool, error) {
				claims, ok := jwtauth.ClaimsFromCtx(ctx)
				if !ok {
					return false, nil
				}
				var c struct {
					Scope string `json:"scope"`
				}
				if err := claims.Decode(&c); err != nil {
					return false, err
				}
				return slices.Contains(strings.Fields(c.Scope), scope), nil
			},
			func(*operation.Context) error {
				return operation.Error{Code: "insufficient_scope", Status: operation.StatusPermissionDenied, Message: "insufficient scope"}
			},
		)
	}
}

Middlewares: []forgo.MiddlewareFactory{http.JWTAuth(jwtConf), forgo.MiddlewareAs(RequireScope("orders:write"))},
```

### Request logging

The base `Recorder` logs every request:

- **On start:** `addr`, `path`, `query`, `method`, `req_headers` and `req_body`. Headers and bodies are capped at 32 KiB, and longer ones carry `headers_cut`/`body_cut`. A body is logged as JSON only when it is whole and valid, compacted so that an indented body stays on one log line, and as a string otherwise, so a `text/plain` or form body keeps the log line JSON.
- **On finish:** `resp_status`, `resp_headers`, `resp_body` and `elapsed`.

The values of the `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie` and `X-Api-Key` headers are always masked as `[*]`. Name more in `SensitiveFields` to mask them in the headers, query, path, JSON bodies and binding errors:

```go
http.Config{
	SensitiveFields: []string{"password", "token", "X-Session-Token"},
	Routes:          routes,
}
```

See [Masking sensitive fields](#masking-sensitive-fields) for what is masked and how.

Disable recording for noisy routes such as health checks:

```go
Args: map[any]any{http.ArgKeyRecorderDisabled: true},
```

Logging limits are exported constants: `RequestRecorderHeaderLimit`, `RequestRecorderBodyLimit`, `ResponseRecorderHeaderLimit`, `ResponseRecorderBodyLimit`.

### Shutdown

`Terminate` closes `State.Terminating`, then calls `http.Server.Shutdown`. The server stops accepting connections and waits for in-flight requests, then for the handlers of hijacked connections, such as WebSockets, which `Shutdown` does not track, until `TermTimeout`. [Long-lived responses and WebSockets](#handlers) must return when `Terminating` closes, or they hold the shutdown until `TermTimeout`. Requests still running then are cut: `Kill` calls `http.Server.Close`, which closes their connections and cancels their contexts, and closes the hijacked connections, whose handlers' reads and writes then fail. The app closes the resources without waiting for the handlers. Combine it with a `TermSig` and an `http.HealthHandler` route for [readiness-aware shutdown](#termination-settings) and [zero-downtime rolling updates on Kubernetes](#zero-downtime-deployments-on-kubernetes).

## gRPC server

Built on [grpc-go](https://github.com/grpc/grpc-go). The `.proto` file is the contract with clients, and the code `protoc-gen-go` and `protoc-gen-go-grpc` (v1.4 or newer) generate from it describes the methods and their messages. Business code never sees that code: a handler is a method of your service that takes and returns structs of your own, which the server converts from and to the proto messages, the way the HTTP server binds a request into a struct and serializes the result. The very method that serves an HTTP route serves a gRPC method. The package `github.com/efegreat/forgo/server/grpc` is named `grpc`, like grpc-go, so alias one of them. This guide imports forgo's as `fgrpc`.

```go
// package user: business code, no transport and no generated type
type GetRequest struct {
	ID     uuid.UUID `uri:"id" validate:"required"`         // the path over HTTP, the id field over gRPC
	Fields []string  `query:"fields"`                       // a query parameter, a google.protobuf.FieldMask
	Tenant string    `header:"X-Tenant" validate:"required"` // a header, the x-tenant metadata
}

type User struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"` // a google.protobuf.Timestamp
}

func (s *Service) Get(ctx context.Context, req GetRequest) (User, error) { ... }
```

```go
// the composition root wires it to both servers
http.RouteConfig{Method: "GET", Path: "/users/{id}", Handler: http.HandlerFor((*user.Service).Get)}
fgrpc.MethodConfig{Method: "GetUser", Handler: fgrpc.HandlerFor((*user.Service).Get)}
```

### Configuration

```go
var GRPC = fgrpc.Config{
	ServerConfig: forgo.ServerConfig{
		Term: forgo.TermConfig{TermSig: &forgo.TermSig{Wait: 10 * time.Second}, TermTimeout: 20 * time.Second},
	},
	Addr:     ":9090",
	Services: []fgrpc.ServiceConfig{users, fgrpc.HealthService()},
}
```

| Field | Default | Description |
| --- | --- | --- |
| `ServerConfig` | | `Name`, termination, server-wide middlewares, `DisableBaseMiddlewares`. |
| `Network` | `"tcp"` | Listener network. |
| `Addr` | `":9090"` | Listen address. |
| `TLSConfig` | | Serves TLS when set. Certificates come from `TLSConfig.Certificates`, `GetCertificate` or `GetConfigForClient`. |
| `MaxRecvMsgSize` | 4 MiB | Request message limit. A larger message is answered `ResourceExhausted` by grpc-go and logged with `err_code: "message_too_large"`. Negative means no limit. |
| `MaxSendMsgSize` | no limit | Response message limit. Sending a larger one fails, and the call is answered `ResourceExhausted`. |
| `MaxConcurrentStreams` | no limit | Calls in flight on one connection. |
| `ConnectionTimeout` | 120s (grpc-go) | Connection setup, TLS handshake included. |
| `Keepalive`, `KeepalivePolicy` | grpc-go's | `keepalive.ServerParameters` and `keepalive.EnforcementPolicy`, passed as they are. |
| `InitialWindowSize`, `InitialConnWindowSize` | dynamic | Flow control windows of streams and connections. Setting either turns off grpc-go's window sizing from the bandwidth-delay product. |
| `MaxHeaderListSize` | 16 MiB (grpc-go) | Request header limit. |
| `StatsHandlers` | | `stats.Handler`s told about connections and calls, such as otelgrpc's tracing and metrics handler. The server receives requests into a value of its own (see [Messages that cannot be decoded](#grpc-errors)), so the `Payload` of a `stats.InPayload` is a `proto.Message` that reads like the request, through `proto.Clone`, `proto.Size` or `ProtoReflect`, but is not of its generated type. Handlers are also told about a request that cannot be decoded. |
| `Observer` | | A `transport.IConnObserver`, as on HTTP, also from the bootstrap. See [`transport`](#transport). An `IConnStateObserver` is told only when connections open and close, since HTTP/2 serves many calls on one connection. An `IConnIOObserver` is not told about connections clients reset once termination started: a grpc-go client closes an idle connection on the GOAWAY of a graceful stop before reading the frames that follow it, so its system resets the connection. An `IConnIOObserver` or an `IConnStateObserver` wraps every connection, which grpc-go then no longer finds to be a `*net.TCPConn`, so on Linux the server sets `TCP_USER_TIMEOUT` on it itself, as grpc-go does on the connections it is handed unwrapped: `Keepalive.Timeout`, 20 seconds by default, unless `Keepalive.Time` is infinite. |
| `Validators` | | Custom validation tags of the default `Validator`, by name, like those of `http.Config`. |
| `Validator` | go-playground `validate` tags, then `Validate() error` | Validates every request before its handler gets it. See [Validation](#validation). |
| `ErrorMapper` | `MapBinding`, `MapValidation`, `MapStatus`, `MapContext` | Maps the errors of handlers and middlewares to `operation.Error`. See [gRPC errors](#grpc-errors). |
| `ErrorDomain` | | The `Domain` of the `errdetails.ErrorInfo` sent with every error, such as your service's DNS name. |
| `SensitiveFields` | | Names whose values are masked as `[*]` in logs. See [Call logging](#call-logging). |
| `LogStreamMessages` | `0`, none | How many messages of each streamed direction of a call are logged, each on a line of its own; negative logs all of them. See [Call logging](#call-logging). |
| `Reflection` | `false` | Serves the server reflection service, v1 and v1alpha, for tools such as `grpcurl`. Its calls are operations like any other, so server middlewares, such as an auth middleware, run on them. |
| `Services` | | Service list. |

There is no option for interceptors: middlewares replace them, and `fgrpc.UnaryInterceptor` and `fgrpc.StreamInterceptor` run grpc-go interceptors as middlewares, inside the chain. See [Middlewares and interceptors](#middlewares-and-interceptors). Tracing and metrics libraries hook in through `StatsHandlers`, and metrics labelled like those of the other servers through an `operation.IObserver` (see [Metrics](#metrics)).

### Services and methods

```go
type ServiceConfig struct {
	Desc        *grpc.ServiceDesc         // generated descriptor, such as &userpb.UserService_ServiceDesc; required
	Middlewares []forgo.MiddlewareFactory // run on every method of the service, after the server's
	Args        map[any]any               // static values in ctx.Args of every call; a method's Args override them
	Methods     []MethodConfig            // the methods the server serves
}

type MethodConfig struct {
	Method      string                    // method name in the .proto file, such as "GetUser"; required
	Name        string                    // operation name; default: the full method, "/user.v1.UserService/GetUser"
	Middlewares []forgo.MiddlewareFactory // run after the service's
	Handler     fgrpc.HandlerFactory      // required
	Args        map[any]any
}
```

```go
var users = fgrpc.ServiceConfig{
	Desc:        &userpb.UserService_ServiceDesc,
	Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewAuth)},
	Methods: []fgrpc.MethodConfig{
		{Method: "GetUser", Handler: fgrpc.HandlerFor((*user.Service).Get)},
		{Method: "WatchUsers", Handler: fgrpc.ServerStreamFor((*user.Service).Watch)},
		{Method: "ImportUsers", Handler: fgrpc.ClientStreamFor((*user.Service).Import)},
	},
}
```

The methods list routes the methods of the `.proto` file to business methods, as HTTP routes route paths. The server serves every configured method itself: forgo builds its own service descriptor from `Desc`, with a chain built once per method. The handlers of the generated descriptor never run, so no type of yours implements the generated `UserServiceServer` interface. Calls to a method that is not configured, of this service or of any other, are answered `Unimplemented` with `errCode` `unimplemented`. They run as the `unimplemented` operation through the base and server middlewares, with the full method logged as `path`.

`New` fails, naming the service and the method, when a `Desc` is missing or configured twice, a service has no methods, a method is configured twice, is not in `Desc` or has no `Handler`, a handler's streaming kind is not the method's, a request or response type does not fit the method's message (see [Messages and structs](#messages-and-structs)), or a handler or middleware cannot be resolved:

```text
grpc: service "user.v1.UserService" method "GetUser": request type: user.GetRequest has no field for user.v1.GetUserRequest.view; add one, or ignore it with a blank field tagged grpc:"view"
```

### Handlers

Every method kind has a handler factory that takes a method of a provided service, whose receiver bootstrap resolves once, in `New`, and one that takes a constructor whose parameters bootstrap resolves. Streams use the standard library's function and iterator types, so business code imports nothing of gRPC:

| Kind | Method of a service | Constructor | Handler |
| --- | --- | --- | --- |
| Unary | `HandlerFor` | `HandlerAs[Req, Resp]` | `func(ctx context.Context, req Req) (Resp, error)` |
| Server streaming | `ServerStreamFor` | `ServerStreamAs[Req, Resp]` | `func(ctx context.Context, req Req, send func(Resp) error) error` |
| Client streaming | `ClientStreamFor` | `ClientStreamAs[Req, Resp]` | `func(ctx context.Context, reqs iter.Seq2[Req, error]) (Resp, error)` |
| Bidi streaming | `BidiStreamFor` | `BidiStreamAs[Req, Resp]` | `func(ctx context.Context, reqs iter.Seq2[Req, error], send func(Resp) error) error` |

```go
func (s *Service) Watch(ctx context.Context, req WatchRequest, send func(User) error) error {
	op, _ := operation.FromCtx(ctx) // ctx is the call's *operation.Context
	terminating := op.State.(*fgrpc.State).Terminating()
	for {
		select {
		case u := <-s.changes:
			if err := send(u); err != nil {
				return err // the client is gone
			}
		case <-terminating:
			return nil // let the server stop gracefully
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) Import(ctx context.Context, users iter.Seq2[User, error]) (Report, error) {
	var report Report
	for u, err := range users { // ends when the client closes its side
		if err != nil {
			return Report{}, err // a request that could not be received, converted or validated
		}
		report.Imported++
	}
	return report, nil
}
```

- `Req` and `Resp` are structs, or pointers to structs, of your own. They may also be the proto messages themselves, such as `*userpb.GetUserRequest`, which are passed as they are, for a hot path or a hand-written layer.
- `ctx` is the call's `*operation.Context`, so loggers, `Args` and `operation.FromCtx` work the same way as on every transport.
- `send` converts a response and sends it. It fails once the client is gone, and the handler returns its error.
- A handler of a bidi stream may range over `reqs` and call `send` from two goroutines.
- A unary or client-streaming handler that returns a nil pointer fails the call with `Internal` and `errCode` `nil_response`.

Each call is one operation, streams included, whose `State` is a `*fgrpc.State`. See [Middlewares and interceptors](#middlewares-and-interceptors).

### Messages and structs

`New` builds the conversion of every request and response type once, from the method's message in the proto registry, where generated code registers it. Each call only runs it.

A message generated by `protoc-gen-go` with the open or hybrid API, the default, is converted by reading and writing its Go struct directly, without protobuf's reflection, which would look every field up and box every value it sets. A message of the opaque API, or of another implementation such as `dynamicpb`, is converted through protobuf's reflection. Both give the same results; only the cost differs.

**Fields match by name.** A struct field maps to the proto field that its `grpc` tag names, or else the field its `json`, `uri` or `query` tag names, or else the field its Go name names. Names compare without case and underscores, so `json:"created_at"`, `json:"createdAt"` and `CreatedAt` all match `created_at`. Embedded structs contribute their fields; embedded pointers are not supported.

| Tag | Effect |
| --- | --- |
| `grpc:"name"` | Maps the field to the proto field `name` exactly. `New` fails when the message has no such field. |
| `grpc:"-"` | Leaves the field out. |
| `json:"-"` | Leaves the field out, unless a `uri`, `query` or `header` tag names it: what JSON hides, gRPC hides too. |
| `header:"X-Tenant"` | On a request, takes the field from the call's metadata (`x-tenant`), as HTTP takes it from the header. A slice of strings takes every value; a pointer stays nil without one. |
| `_ struct{} grpc:"name"` | A blank field that ignores the proto field `name` of a request. |

**Nothing is dropped silently.** `New` fails when a field of a request message has no struct field, since its value would be lost, or when an exported field of a response struct has no proto field, since the client would never get it. Ignore the former with a blank field and leave the latter out with `grpc:"-"`. A request struct may have fields the message lacks, filled by middlewares or metadata, and a response message may have fields the struct lacks, which keep their default.

**Types.**

| Proto | Go |
| --- | --- |
| `string` | `string` types, or any type that implements `encoding.TextUnmarshaler` (request) and `encoding.TextMarshaler` (response), such as `uuid.UUID` or `netip.Addr`; the empty string is the zero value |
| `bytes` | `[]byte` |
| `bool` | `bool` |
| integers of any size and sign | any integer type; a value out of the range of its destination fails |
| `float`, `double` | `float32`, `float64` |
| `enum` | an integer type (the number), or a string type (the value's name, such as `"STATUS_ACTIVE"`) |
| message | a struct or a pointer to one, converted recursively, or the generated message itself, passed as it is |
| `repeated T` | a slice of T's type, pointers to structs included |
| `map<K, V>` | a map |
| `google.protobuf.Timestamp` | `time.Time`, in UTC |
| `google.protobuf.Duration` | `time.Duration` |
| wrappers, such as `google.protobuf.StringValue` | the type of their value; a pointer for presence |
| `google.protobuf.Struct`, `Value`, `ListValue` | `map[string]any`, `any`, `[]any`, as `structpb` converts them |
| `google.protobuf.FieldMask` | `[]string` of its paths |
| `google.protobuf.Any` | `*anypb.Any`, passed as it is |
| a `oneof` | a pointer field per member; at most one may be set in a response |

**Presence.** A pointer is nil when its field is not set. Any other type keeps its zero value when its field is not set, and a zero value is not sent. As in proto3, a scalar without `optional` is not set when it is zero, so an enum of string type gets `""` for the value 0, and sends `""` as 0. Use a pointer, with `optional` or a wrapper in the `.proto`, to tell zero from absent.

**At runtime**, a request value the struct cannot hold, such as a number out of its range or a string `UnmarshalText` rejects, is answered `InvalidArgument` with `errCode` `binding` and the message `invalid field: address.zip_code` (or `invalid metadata: x-tenant`). The cause is only logged, and masked when a name of the path is in `SensitiveFields`. A response the message cannot hold, such as an enum name the `.proto` does not know or two members of a `oneof`, is a server bug: `Internal` with `errCode` `invalid_response`, logged with the field.

Converting costs about a microsecond and a dozen allocations per call for a message of a few fields, measured by `BenchmarkUnary` against proto-typed handlers.

### Validation

Before a handler gets a request, `Config.Validator` validates it: the request of a unary or server-streaming method, and every request a client or bidi stream receives, whose range then ends with the error. It gets a pointer to the handler's request type. The default runs go-playground/validator on the `validate` tags of a struct, as the HTTP binder does, with the custom tags of `Validators`, then calls the `Validate() error` method of a request that has one, such as a message [protoc-gen-validate](https://github.com/bufbuild/protoc-gen-validate) generates. `New` validates an empty value of every request type, nested structs included, so a tag the server does not know, such as a custom one only `http.Config.Validators` registers, panics at startup rather than on the first call. The default is skipped on a request type it has nothing to check on: no `validate` tag on it or on the structs it holds, and no `Validate` method. A `Validator` of your own runs on every request.

A failure is answered `InvalidArgument` with `errCode` `validation`. The message of a `validate` tag failure names every failed field and rule, such as `field: name, rule: required`, as over HTTP; that of any other error is its text, so the client learns what to fix. Plug in another validator for proto-typed handlers, such as [protovalidate](https://github.com/bufbuild/protovalidate-go):

```go
pv, err := protovalidate.New()
// ...
Validator: func(req any) error {
	if m, ok := req.(proto.Message); ok {
		return pv.Validate(m)
	}
	return nil
},
```

A function that always returns nil turns validation off.

### Middlewares and interceptors

A middleware is an `operation.Middleware`, as on every transport, and reads the call's `State`, which holds what a grpc-go interceptor gets:

```go
type State struct {
	FullMethod string        // "/user.v1.UserService/GetUser"
	Request    proto.Message // the request of a unary or server-streaming method; replaceable before next
	Response   proto.Message // the response of a unary or client-streaming method; replaceable, sent after the chain
}

func (s *State) Metadata() metadata.MD          // a copy of the metadata the client sent
func (s *State) Peer() (*peer.Peer, bool)       // the client
func (s *State) SetHeader(md metadata.MD) error // response header, sent with the first message
func (s *State) SendHeader(md metadata.MD) error
func (s *State) SetTrailer(md metadata.MD)       // response trailer, sent with the status
func (s *State) WrapStream(wrap func(grpc.ServerStream) grpc.ServerStream)
func (s *State) ClientStreams() bool             // the client sends a stream
func (s *State) ServerStreams() bool             // the method sends a stream
func (s *State) Terminating() <-chan struct{}   // closed when the server starts terminating
func (s *State) Code() codes.Code               // the status the call ended with, so far
```

- The request of a unary or server-streaming method is received before the chain runs, so middlewares and the request log read `State.Request`, the proto message. A middleware may replace it with another message of the method's type. The terminal handler converts and validates it after the middlewares, like an HTTP request is bound after them. The terminal of a server-streaming method receives it again through the stream first, as grpc-go's generated handlers do, so that stream wrappers see it.
- The `Responder` of the base chain sends `State.Response` once the chain returns without an error. A middleware may replace it after `next` returns, or set it and return without calling `next`, to answer from a cache. Unary and client-streaming responses are sent there, inside the `Recorder`, so a failed send is logged as a failure.
- `WrapStream` replaces the stream the rest of the chain and the handler receive and send on, as a stream interceptor wraps it. A wrapper sees the request of a server-streaming method when the handler receives it, and may change or reject it, like a validating interceptor's wrapper. The request of a unary method never goes through the stream, as grpc-go runs no stream interceptor on unary methods: middlewares read `State.Request`.
- To pass a value to the handler, set it in `ctx.Args`: the handler's `ctx.Value` finds it.

```go
func NewAuth(verifier *auth.Verifier) operation.Middleware {
	return middleware.Guard(
		func(ctx *operation.Context) (bool, error) {
			st, ok := ctx.State.(*fgrpc.State)
			if !ok {
				return false, nil
			}
			tokens := st.Metadata().Get("authorization")
			return len(tokens) == 1 && verifier.Valid(tokens[0]), nil
		},
		func(*operation.Context) error {
			return operation.Error{Code: "unauthenticated", Status: operation.StatusUnauthenticated, Message: "unauthenticated"}
		},
	)
}
```

**Interceptors.** `fgrpc.UnaryInterceptor` and `fgrpc.StreamInterceptor` run any grpc-go interceptor, such as one of [go-grpc-middleware](https://github.com/grpc-ecosystem/go-grpc-middleware)'s, as a middleware, at its place in the chain, so the `Recorder` logs the calls it rejects and the `Responder` answers them like any other error. `UnaryInterceptorAs` and `StreamInterceptorAs` take a constructor whose parameters bootstrap resolves.

```go
var GRPC = fgrpc.Config{
	ServerConfig: forgo.ServerConfig{Middlewares: []forgo.MiddlewareFactory{
		fgrpc.UnaryInterceptor(auth.UnaryServerInterceptor(authenticate)),
		fgrpc.StreamInterceptor(auth.StreamServerInterceptor(authenticate)),
	}},
	// ...
}
```

- A unary interceptor runs on unary methods only, and a stream interceptor on the others, like in grpc-go. Each lets the other kind's calls through.
- A unary interceptor's request is `State.Request`, and the response it returns becomes `State.Response`, so it may answer without calling its handler. The context it passes to its handler becomes the operation's context until the handler returns.
- A stream interceptor's stream becomes the one the handler receives and sends on, and its context the operation's, until its handler returns. Its wrapper sees every request message, the one of a server-streaming method included, like in grpc-go.
- `UnaryServerInfo.Server` and the `srv` of a stream interceptor are nil, since forgo serves no generated service.

`http.JWTAuth` and `http.RateLimit` read `*http.State`, so they serve HTTP only. On gRPC, use a `Guard` like the one above, or go-grpc-middleware's `auth` and `ratelimit` interceptors.

### Authentication and rate limiting

`fgrpc.JWTAuth`, `fgrpc.RateLimit` and `fgrpc.RateLimitWith` are the gRPC versions of the [HTTP middlewares](#built-in-http-middlewares) of the same names. Both servers' middlewares are thin layers over `github.com/efegreat/forgo/jwtauth`, which verifies tokens, and `github.com/efegreat/forgo/ratelimit`, which counts calls, so one key set, one limiter and the code that reads claims serve both. Each takes its config and returns a `forgo.MiddlewareFactory`. The config is validated once, and an invalid one fails `New`.

```go
var GRPC = fgrpc.Config{
	ServerConfig: forgo.ServerConfig{
		Middlewares: []forgo.MiddlewareFactory{
			fgrpc.RateLimit(fgrpc.RateLimitConfig{Limit: 100, Period: time.Minute}),
		},
	},
	Services: []fgrpc.ServiceConfig{
		fgrpc.HealthService(), // probes call it without a token
		{
			Desc: &userpb.UserService_ServiceDesc,
			Middlewares: []forgo.MiddlewareFactory{
				fgrpc.JWTAuth(fgrpc.JWTConfig{
					Keys:     &jwtauth.JWKS{URL: "https://idp.example.com/.well-known/jwks.json"},
					Issuer:   "https://idp.example.com/",
					Audience: []string{"shop-api"},
				}),
			},
			Methods: []fgrpc.MethodConfig{{Method: "GetUser", Handler: fgrpc.HandlerFor((*user.Service).Get)}},
		},
	},
}
```

**JWT authentication.** The token is read from the `authorization` metadata as `Bearer <token>`, which grpc-go's per-RPC credentials, such as `oauth.TokenSource`, send. `JWTConfig` has the fields of the HTTP `JWTConfig` but `Cookie` (`Keys`, `Issuer`, `Audience`, `Leeway`, `Optional`), and tokens are checked by the same rules. A stream is authenticated once, before its handler runs. Handlers read the claims with `jwtauth.ClaimsFromCtx`. Attach `JWTAuth` to the services that need it rather than to the server when the server serves `HealthService()` or reflection, which probes and tools call without a token.

| Call | Code | `errCode` |
| --- | --- | --- |
| No token | `Unauthenticated` | `unauthenticated` |
| Expired token | `Unauthenticated` | `token_expired` |
| Any other invalid token | `Unauthenticated` | `invalid_token` |
| Keys cannot be fetched | `Unavailable` | `auth_unavailable` |

**Rate limiting.** `RateLimitConfig` has the fields of the HTTP `RateLimitConfig`, with `DisableMetadata` for `DisableHeaders`. The zero value admits 60 calls per minute per client IP. The default key is the IP of the call's peer, counted by its /64 network for IPv6. Behind `TrustedProxies`, it is the client the `x-forwarded-for` metadata names, read right to left like the HTTP header; without them, the metadata is ignored. A peer that has no IP, such as a Unix socket, is counted by its address. A stream counts once, when it starts.

- Admitted calls get the `ratelimit-limit`, `ratelimit-remaining` and `ratelimit-reset` header metadata, unless `DisableMetadata` is set. Clients read it with `grpc.Header(&md)`.
- A rejected call is answered `ResourceExhausted` with `errCode` `rate_limited` and the `retry-after` header metadata, in seconds.
- When the limiter fails, the call is answered `Unavailable` with `errCode` `rate_limit_unavailable`, unless `FailOpen` admits it.

`RateLimit` counts in its own `ratelimit.Memory`: attached to the server, the limit covers every method together, unknown methods included; attached to a service or a method, that one alone. `RateLimitWith` counts in a `ratelimit.ILimiter` that bootstrap resolves, such as forgo-adapter's Redis limiter, which an HTTP server's `RateLimitWith` may share with another `Prefix`.

### gRPC errors

Errors are answered the way HTTP answers them, mapped to an `operation.Error`, then sent as a status: the gRPC code of its `Status` (see [Statuses](#errors)), its `Message`, and an [`errdetails.ErrorInfo`](https://pkg.go.dev/google.golang.org/genproto/googleapis/rpc/errdetails#ErrorInfo) whose `Reason` is its `Code` and whose `Domain` is `Config.ErrorDomain`. The cause stays private and is only logged.

```go
// client side
st := status.Convert(err)
for _, d := range st.Details() {
	if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetReason() == "name_taken" { ... }
}
```

- An error that maps to nothing becomes `Internal` with `errCode` `internal` and the message `an error occurred`. So does a panic, which `Recovery` recovers, and whose stack is logged.
- The request log has the handler's error itself, with what is wrapped around its `operation.Error`, and `grpc_code`, the code the client got.
- `DisableBaseMiddlewares` drops the `Responder` from the chain; the transport then sends the response, or answers the error, after the chain. To add it to your own chain, use `fgrpc.Responder(errorMapper, domain)`.

Default error mappers, all exported:

| Mapper | Matches | `errCode` | Code | Message |
| --- | --- | --- | --- | --- |
| `fgrpc.MapBinding` | a request value the handler's struct cannot hold | `binding` | `InvalidArgument` | `invalid field: <path>` or `invalid metadata: <key>` |
| `fgrpc.MapValidation` | a `Validator` failure | `validation` | `InvalidArgument` | `field: <name>, rule: <rule>` for each failed `validate` tag, else the validator's error text |
| `fgrpc.MapStatus` | an error that carries a gRPC status, such as `status.Error(codes.NotFound, "…")`, the error of a gRPC call to another service, or the rejection of an interceptor | the `Reason` of its `ErrorInfo`, else the code in snake case, such as `not_found` | its own | its own |
| `fgrpc.MapContext` | `context.Canceled`, `context.DeadlineExceeded` | `canceled`, `deadline_exceeded` | `Canceled`, `DeadlineExceeded` | `call canceled`, `deadline exceeded` |

A status that `MapStatus` matched is sent **as it is**, details included, so handlers that build statuses with `errdetails.BadRequest` or `RetryInfo` keep them. Its severity is critical for `Unknown`, `Internal` and `DataLoss`, moderate for `Unavailable` and light for the other codes. A status error of a downstream service that a handler returns unwrapped reaches the client as it is too; wrap it in an `operation.Error` to answer with your own code and message. Setting `ErrorMapper` **replaces** the defaults, so include them if you still want them.

**Failed receives and sends.** grpc-go answers a message it cannot receive or send itself, with its own status and without `ErrorInfo`, the moment `RecvMsg` or `SendMsg` fails. Nothing can answer such a call again, so forgo records the failure in `State`, logs it with the code grpc-go sent, and fails the operation even when the handler ignored the error:

| `errCode` | grpc-go's code | When | Severity |
| --- | --- | --- | --- |
| `message_too_large` | `ResourceExhausted` | A request message is over `MaxRecvMsgSize` | light |
| `invalid_message` | usually `Internal` | A unary or server-streaming method got zero or several request messages | light |
| `send_failed` | its own, such as `ResourceExhausted` over `MaxSendMsgSize` | A response message cannot be sent | moderate |
| `canceled`, `deadline_exceeded` | `Canceled`, `DeadlineExceeded` | The client canceled, or its deadline passed, during a receive or send | light |

A stream that a middleware wrapped may fail a receive or send itself, such as a validating interceptor's. The server returns its status to grpc-go, which sends it, since nothing was sent yet.

**Messages that cannot be decoded.** The server decodes request messages itself, with the codec registered for protobuf, so a request message that cannot be decoded, such as a truncated one or one whose string field is not UTF-8, is answered like a binding error: `InvalidArgument`, with `errCode` `invalid_message`, light. grpc-go alone would answer `Internal`. The message holds protobuf's reason, as grpc-go's and connect-go's do, which names at most a field and never a value, such as `invalid request message: proto: field user.v1.CreateUserRequest.name contains invalid UTF-8`. Middlewares still run on such a call, with a nil `State.Request`, so an auth middleware rejects it first. A stream's wrappers get the error from `RecvMsg`, and the requests of a client or bidi stream end with it, which the handler returns. The server decodes every call with this codec, whatever its content-subtype, so codecs registered for other subtypes, such as a JSON codec, are not used.

### Metadata and the request ID

The operation ID is the incoming `x-request-id` metadata (`fgrpc.ReqIDMetadata`) when it is at most 128 bytes of visible ASCII, and a new UUIDv7 otherwise. The server returns it in the `x-request-id` trailer of every call, failed ones included:

```go
var trailer metadata.MD
resp, err := client.GetUser(ctx, req, grpc.Trailer(&trailer))
id := trailer.Get("x-request-id")
```

### Health and reflection

`fgrpc.HealthService()` serves the [gRPC health checking protocol](https://grpc.io/docs/guides/health-checking/), `grpc.health.v1.Health`, from the server's `TermSig`, like `http.HealthHandler`: `SERVING` while the server runs, and `NOT_SERVING` once the `TermSig` is signaled, so that Kubernetes gRPC probes and client-side load balancing stop sending calls during `TermSig.Wait`. Without a `TermSig` it always serves.

```go
health := fgrpc.HealthService()
health.Args = map[any]any{fgrpc.ArgKeyRecorderDisabled: true} // no log line per probe

var GRPC = fgrpc.Config{
	ServerConfig: forgo.ServerConfig{Term: forgo.TermConfig{TermSig: &forgo.TermSig{Wait: 10 * time.Second}}},
	Services:     []fgrpc.ServiceConfig{users, health},
}
```

```yaml
readinessProbe:
  grpc: {port: 9090}
  periodSeconds: 2
  failureThreshold: 1
```

- `Check` knows the empty service name, for the whole server, and the name of every service of the server. It answers `NotFound` with `errCode` `unknown_service` for any other name.
- `Watch` sends the current status, then `NOT_SERVING` when the `TermSig` is signaled (`TermSig.Done`), and ends when the client cancels or the server terminates, so it never holds up the graceful stop. It sends `SERVICE_UNKNOWN` for an unknown name.
- `List` returns the status of every name `Check` knows.

`Reflection: true` serves server reflection, so `grpcurl -plaintext localhost:9090 list` works. It lists the services of the server and reads their descriptors from the proto registry.

### Call logging

The base `Recorder` logs every call:

- **On start:** `addr` (the peer), `path` (the full method), `req_headers` (the metadata) and `req_body` (the request message of a unary or server-streaming method).
- **On finish:** `grpc_code`, the name of the status code the client got, such as `"NotFound"`, `resp_headers` and `resp_trailers` (the header and trailer metadata the call set, when it set any), `resp_body` (the response message of a unary or client-streaming method), `req_msgs` (the messages a client or bidi stream received), `resp_msgs` (the messages a server or bidi stream sent) and `elapsed`.

Response metadata is logged however it was set: through `State.SetHeader`, `SendHeader` and `SetTrailer`, a stream's own methods, or `grpc.SetHeader`, `grpc.SendHeader` and `grpc.SetTrailer` on the operation's context, by a handler, a middleware or an interceptor. The `x-request-id` trailer is added once the operation ended, so it is not in `resp_trailers`; the line has it as `op_id`. Metadata set after the end line, such as by a goroutine still running, is sent but not logged.

The messages, not the structs they convert to, are logged as JSON with their proto field names, as `protojson` with `UseProtoNames` writes them, compacted. The server writes that JSON itself, in one pass that masks as it goes, without the allocations of `protojson`. The well-known types with a JSON form of their own, other than wrappers, `Timestamp` and `Duration`, and messages that can have extensions, are encoded by `protojson`. A message larger than 32 KiB on the wire is not encoded at all, and only `body_cut: true` is logged. A message `protojson` cannot encode, such as one with a string that is not valid UTF-8, is logged without its body. Binary metadata (`-bin` keys) is logged in base64.

The other messages of a stream are logged only with `LogStreamMessages`, which a long-lived stream would otherwise flood the logs with. It logs the first `LogStreamMessages` messages of each streamed direction, all of them when it is negative, at info, masked and capped like the messages above:

- `stream message received`, for the requests of a client or bidi stream, with `kind: "request"`, `req_body` and `stream_seq`, the message's number from 1.
- `stream message sent`, for the responses of a server or bidi stream, with `kind: "response"`, `resp_body` and `stream_seq`.

The logger wraps the stream below every middleware's, so it logs the messages as they come off and go on the wire. It is part of the base chain, so `DisableBaseMiddlewares` and `ArgKeyRecorderDisabled` turn it off too.

The `authorization`, `proxy-authorization`, `cookie`, `set-cookie` and `x-api-key` metadata are always masked as `[*]`. `SensitiveFields` masks more metadata and message fields by their proto names, at any depth: `card_number` masks `{"payment":{"card_number":"[*]"}}`. Disable recording for noisy methods such as health checks with `Args: map[any]any{fgrpc.ArgKeyRecorderDisabled: true}`. Logging limits are exported constants: `RequestRecorderHeaderLimit`, `RequestRecorderBodyLimit`, `ResponseRecorderHeaderLimit` (each of the header and the trailer), `ResponseRecorderBodyLimit`.

### Shutdown

`Terminate` closes `State.Terminating`, then calls grpc-go's `GracefulStop`: the server stops accepting connections and calls, sends GOAWAY, and waits until every handler has returned, even one whose client went away, until `TermTimeout`. Long-lived streams, such as a `Watch`, must return when `Terminating` closes, or they hold the shutdown until `TermTimeout`. Then `Kill` calls `Stop`, which closes every connection and cancels the context of the calls still running, and the app closes the resources without waiting for their handlers.

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
			Lock:     operation.Lock{Key: "job:nightly-report", TTL: time.Hour},
			Job:      cron.As(report.NewNightlyJob),
		},
		{Name: "heartbeat", Schedule: "@every 30s", Job: cron.As(NewHeartbeat)},
	},
}
```

| `JobConfig` field | Description |
| --- | --- |
| `Name` | Operation name, as `op_name` in logs. Required and unique. |
| `Schedule` | Standard 5-field spec (`minute hour day-of-month month day-of-week`), a descriptor (`@yearly`, `@monthly`, `@weekly`, `@daily`, `@hourly`), or `@every <duration>`. Prefix with `CRON_TZ=<zone>` for a time zone; the default is the process's local time. |
| `Lock` | Optional distributed lock, so a job runs on one replica at a time. Requires an `operation.ILocker` provider; without one, the server fails at startup. A replica that loses the race skips the run. |
| `Middlewares` | Middlewares of this job, inside the server's. |
| `Job` | `cron.As(constructor)`. The constructor's dependencies are resolved by bootstrap, and it returns a `cron.IJob`. |

`New` fails at startup, naming the job, for a missing name or `Job`, a duplicate name, a schedule it cannot parse, and dependencies it cannot resolve.

```go
type IJob interface {
	Tolerance() operation.Tolerance // read once, when the server is created
	Run(ctx context.Context) error  // ctx is the run's *operation.Context
}

type ExpireJob struct {
	carts  *cart.Service
	alerts *alert.Client
}

func NewExpireJob(carts *cart.Service, alerts *alert.Client) *ExpireJob {
	return &ExpireJob{carts: carts, alerts: alerts}
}

// Tolerance is a method, so that its hooks can use the job's dependencies.
func (j *ExpireJob) Tolerance() operation.Tolerance {
	return operation.Tolerance{
		Retry: true, RetryAttempts: 3, RetryDelay: 10 * time.Second,
		OnFailure: func(ctx *operation.Context, err error) { j.alerts.Notify(ctx, "cart expiry failed", err) },
	}
}

func (j *ExpireJob) Run(ctx context.Context) error {
	return j.carts.ExpireStale(ctx)
}
```

Notes:

- Return `operation.Tolerance{}` for no retries, or `operation.DefaultTolerance` for 5 attempts, 5s apart.
- robfig starts each run in its own goroutine, so runs of a job overlap when one outlasts the schedule's interval. Set a `Lock` to skip a run while another run holds it, in this process or another.
- Runs missed while no process was up, for example during a deployment at the scheduled time, are not caught up.
- `Tolerance` and `Lock` are applied by the base chain, so `DisableBaseMiddlewares` ignores them. Retries re-run everything inside the `Tolerance` middleware, including server and job middlewares. The lock is held across all attempts and is not renewed, so its `TTL` must cover `RetryAttempts × (run time + RetryDelay)`. One `operation started`/`completed` pair is logged per run.
- A run whose lock cannot be acquired, for example because the locker's backend is down, does not run and is logged as `failed to acquire operation lock`.
- Panics are recovered by the `Recovery` middleware of the base chain. With `DisableBaseMiddlewares`, add `middleware.Recovery` to your own chain: robfig runs each job in its own goroutine, so an unrecovered panic crashes the process.
- Jobs receive a context that is **not** cancelled when termination starts. `Terminate` stops scheduling and waits for running jobs. When they do not finish within `TermTimeout`, the app kills the server, and `Kill` cancels their context: retry waits end, and jobs that honor their context stop. Give long jobs their own timeouts and size `TermTimeout` accordingly.

---

## Worker

Broker-agnostic message handling, made of two servers:

- **`worker.ConsumerConfig`** consumes messages from a broker and dispatches them to typed handlers. An **inbox** table makes processing idempotent, and attempts are counted per subscriber.
- **`worker.ProducerConfig`** is an **outbox** relay. It publishes messages your application stored in an outbox table, in the same transaction as the business change, so nothing is lost or published for a rolled-back change.

They can run in one app or in separate deployments. Both need a broker and SQL storages. [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md) provides RabbitMQ, Google Cloud Pub/Sub and Redis Streams brokers with optional dead-letter queues and PostgreSQL and MySQL inbox and outbox storages, or you implement the [broker](#broker-contracts) and [storage](#storage-contracts) contracts yourself.

### Messages

```go
type Message struct {
	ID          string                 // unique id (UUIDv7 from NewMessage)
	Name        string                 // routes to the handler (consumer) and publisher (producer)
	Payload     []byte                 // JSON body
	Headers     *types.JSONMap[string] // string headers
	CreatedAt   time.Time
	Key         string // the routing key on RabbitMQ, the ordering key on Pub/Sub
	Destination string // where to publish, e.g. an exchange
}

msg := worker.NewMessage("order.placed", "order.placed", "orders", payload) // name, key, destination, payload
```

**What `Key` does depends on the broker.** forgo-adapter's RabbitMQ broker publishes with it as the routing key, which the bindings of the queues match: the key `order.placed` reaches a queue bound to `orders` with `order.*`, while an order's ID would match no binding, and RabbitMQ would drop the message. On Pub/Sub, it is the ordering key, such as the order's ID, for subscriptions with `Ordering`.

Broker adapters carry identity across the wire with the headers `worker.MessageIDHeader` (`x-msg-id`), `worker.MessageNameHeader` (`x-msg-name`) and `worker.MessageTimestampHeader` (`x-msg-timestamp`, Unix nanoseconds). On the consuming side they rebuild the message with `worker.NewMessageFromDelivery`, which leaves `CreatedAt` zero without a valid, positive timestamp header; the consumer then logs no `msg_elapsed`.

**The route between a `Destination` and a subscriber is the application's.** The broker topology, which binds a message's destination to the queue or subscription a subscriber reads, is declared in the broker's configuration or the infrastructure, not by forgo. Application tests do not cover it (see [Brokers in tests](#brokers-in-tests)): a destination no subscriber reads reaches no handler while every test passes. Verify it where it is declared, in staging or smoke checks.

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
					Lock:                operation.Lock{Key: "order-cancelled", TTL: time.Minute},
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

An error retries the message until `MaxAttempt`. When retrying cannot help, for example because the message is about an order that no longer exists, wrap `worker.ErrNoRetry`: the message is marked *failed* and rejected at once.

```go
func (s *Service) OnOrderPlaced(ctx context.Context, evt OrderPlaced) error {
	order, err := s.orders.Get(ctx, evt.OrderID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("order %s not found: %w", evt.OrderID, worker.ErrNoRetry)
	}
	...
}
```

| Field | Default | Description |
| --- | --- | --- |
| `ConsumerConfig.ServerConfig` | | `Name`, termination, server-wide middlewares. |
| `ConsumerConfig.Binder` | JSON + validator | `worker.IBinder` that decodes `Message` into `Req`. The default decodes the payload as JSON, then validates a struct request, also behind pointers, with its `validate` tags. Other request types, such as slices, maps or `json.RawMessage`, are only decoded. An empty payload is not decoded, so the request keeps its zero value. |
| `ConsumerConfig.Validators` | | Custom validation tags of the default binder, by tag name, as for the [HTTP server](#request-binding-and-validation). |
| `ConsumerConfig.SensitiveFields` | | Names whose values are masked as `[*]` in the logged headers, payload and binding errors. The credential headers, such as `Authorization`, are always masked. See [Masking sensitive fields](#masking-sensitive-fields). |
| `ConsumerConfig.Clean` | see below | Inbox cleanup. |
| `SubscriberConfig.Name` | | Passed to `IBroker.NewSubscriber`. Unique and non-empty: the inbox keeps the messages of each subscriber apart by it. |
| `SubscriberConfig.Concurrency` | `1` | Messages handled in parallel. |
| `SubscriberConfig.MaxAttempt` | `3` | Attempts before a message is marked failed and rejected. |
| `SubscriberConfig.AckTimeout` | `3s` | Time limit for each acknowledgement sent to the broker: complete, retry or reject. |
| `SubscriberConfig.Middlewares` | | Middlewares for every handler of the subscriber. |
| `HandlerConfig.Name` | | The `Message.Name` the handler receives. Unique and non-empty within its subscriber. |
| `HandlerConfig.Lock` | | Per-message lock. The key becomes `<Key>-<subscriber>-<messageID>`. If another delivery holds it, the message is retried (`msg_locked`, logged as a warning) after the broker's [retry delay](#retry-delays). It is not acknowledged, because the holder may have crashed and left its lock until it expires. |
| `HandlerConfig.TransactionDisabled` | `false` | Skip the `Transactor`. |
| `HandlerConfig.Timeout` | no limit | Ends the context your handler receives. An error the handler returns after that fails with `msg_timeout`, unless it is an `operation.Error`, and is retried like any handler error. Go cannot stop a goroutine: a handler that ignores its context keeps running and holds its concurrency slot. The inbox mark and the commit that follow the handler are not bound by it. |
| `HandlerConfig.Middlewares` | | Handler middlewares. |
| `HandlerConfig.Args` | | Static values in `ctx.Args` of every message. Supports `worker.ArgKeyRecorderDisabled`. |

`New` rejects an empty or duplicate subscriber or handler name, a handler without `Handler`, an invalid validator, and a negative `Concurrency`, `MaxAttempt`, `HandlerConfig.Timeout`, `Clean.CleanupLimit` or `Clean.CleanupInterval`, which would otherwise only fail while serving. Errors name the subscriber and the handler.

**Required dependencies:** `worker.IBroker` and `worker.InboxStorage`. `operation.Transactor` is needed unless every handler sets `TransactionDisabled`. Your inbox storage must join that transaction, so that the handler's writes and the *completed* mark commit together. forgo-adapter's `sqltx.Transactor` and SQL inbox storages do. `operation.ILocker` is needed only when a handler sets `Lock`. A missing dependency fails at startup and names the handler that needs it.

A subscriber that stops consuming on its own, for example because its broker gave up reconnecting, makes `Serve` return its error. So does one whose `Consume` returns without an error before the server stops. That stops the app, so that an orchestrator restarts it instead of leaving it up without consuming.

**Shutdown.** Terminating the consumer stops consuming and waits for the messages in flight. Their handlers run on a context that terminating does not cancel, so they finish. When the server's `TermTimeout` runs out first, `App` kills the server, which cancels the context of the handlers still running. A handler that stops on it fails like on any error, and its message is retried.

**Processing a message:**

1. The subscriber receives a delivery and picks the handler by `Message.Name`. A message whose name has no handler runs as the operation `not_found`, with its name logged as `msg_name`, and is rejected (no redelivery) with code `msg_no_handler`. Producers choose message names, so they never become operation names on their own.
2. Base middlewares run: `ID`, `Logger` (adds `msg_id`), `Locker`, `Recorder` (logs headers and payload with `SensitiveFields` and the credential headers masked, then `msg_elapsed` since `CreatedAt`), `Observe` (with an `operation.IObserver`), `Recovery`, and the **idempotency guard**. The guard looks the message up in the inbox by `(message ID, subscriber)`. A message already completed is acknowledged and skipped. A message already *failed* or *rejected* is rejected again without running, because the inbox marks a message before the broker rejects it: when the rejection itself fails, for example because the dead-letter copy cannot be stored, the redelivery repeats it instead of dropping the message.
3. Server, subscriber and handler middlewares run.
4. The inbox attempt counter is incremented. A new message is recorded as *waiting*. A message whose count passes `MaxAttempt` is marked *failed* and rejected without running the handler (`msg_attempts`). That only happens when earlier attempts ended without a verdict, for example because the message crashed the process. Otherwise such a message would crash every consumer it reaches, forever.
5. The payload is bound into `Req`. On failure the message is marked *rejected* and rejected at the broker, with no retry (`msg_bind`).
6. `Transactor.Do` runs your handler, then marks the inbox row *completed*. Both run on the transaction's context. With `HandlerConfig.Timeout`, the handler's context ends after it.
7. **Success:** the delivery is acknowledged. **Error or panic:** while attempts are below `MaxAttempt`, the delivery is retried, after the broker's [retry delay](#retry-delays) for the failed attempt. At `MaxAttempt`, or at once for an error that wraps `worker.ErrNoRetry`, it is marked *failed* and rejected.

The `operation completed` or `operation failed` line of a message logs the attempt as `msg_attempt`, so a failure that is retried can be told apart from one at the last attempt. It is 0 when processing failed before the attempt was counted.

Every delivery is settled (acknowledged, retried or rejected) exactly once, so it never stays in flight at the broker until the broker gives up on it, which can take until its connection drops or its acknowledgement deadline expires and, with a prefetch limit, stalls the subscriber:

- When processing fails before step 7 settles the delivery, for example on an inbox, lock or middleware error or a lock held by another delivery, the message is logged (`message left unsettled, retrying`) and retried. This retry does not count as an attempt. Without a [retry delay](#retry-delays) on the broker, an inbox or locker outage becomes a tight redelivery loop.
- A middleware that returns `nil` without calling `next` has its delivery acknowledged. A middleware that drops a message for good settles it itself, for example with `ctx.State.(*worker.State).Delivery.Reject(ctx)`. Otherwise its error retries the message forever.
- `State.Delivery` settles at most once. After the first `Complete`, `Retry` or `Reject`, the others return `worker.ErrAlreadySettled` without reaching the broker. RabbitMQ, for example, closes the channel on a second acknowledgement.

A rejected message is dropped by the broker unless its queue has a dead-letter queue. With forgo-adapter's RabbitMQ broker, set [`DeadLetter`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#dead-letter-queues) on the queue to keep messages rejected in steps 1, 5 and 7. With its Pub/Sub broker, set [`DeadLetter`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#dead-letter-topics) on the subscription, which publishes them to a dead-letter topic. With its Redis Streams broker, set [`DeadLetter`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#dead-letter-streams) on the subscriber, which copies them to a dead-letter stream.

#### Retry delays

forgo does not wait before a retry, so a retry holds no concurrency slot. It passes the attempt that failed, as counted by the inbox, to `IDelivery.Retry`, and the broker decides when the message comes back. The attempt is 0 for a retry before the attempt was counted, such as on a lock conflict or an inbox outage.

**Without a delay configured on the broker, a retried message is delivered again at once.** A downstream outage of a few milliseconds then uses up every `MaxAttempt`, and a lock conflict or an inbox outage redelivers in a tight loop until it is resolved. Configure the delays on the broker, without plugins:

| Broker | How it delays |
| --- | --- |
| RabbitMQ (forgo-adapter) | [`QueueConfig.Retry`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#retry-queues) declares retry queues whose message TTL is the delay: `Initial`, doubling with every attempt, up to `Max`. |
| Pub/Sub (forgo-adapter) | [`SubscriptionConfig.Retry`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#retry-policy) sets the subscription's retry policy: a backoff from `Initial`, growing exponentially with every delivery, up to `Max`. It cannot delay single messages, so the attempt is not used. |
| Redis Streams (forgo-adapter) | [`SubscriberConfig.Retry`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#delayed-retries) leaves the entry pending, idle for so long that the group claims it again once the delay has passed: `Initial`, doubling with every attempt, up to `Max`, at most the subscriber's `ClaimIdle`. |
| NATS JetStream | `NakWithDelay` with a delay for the attempt, or the consumer's `BackOff`. |
| SQS | The message's visibility timeout, set to the delay for the attempt. |

Brokers that count deliveries stop redelivering after a limit of their own: a quorum queue's delivery limit (20 by default since RabbitMQ 4.0), NATS `MaxDeliver`, the SQS redrive `maxReceiveCount`, and Pub/Sub's dead-letter maximum (forgo-adapter's `DeadLetterConfig.MaxDeliveryAttempts`). Set it above `MaxAttempt` plus room for the retries that count no attempt. A copy published to a RabbitMQ retry queue starts a fresh count.

#### Retrying failed messages

A message that reaches `MaxAttempt`, or whose handler failed it with `worker.ErrNoRetry`, is marked *failed* and rejected, so it moves to the dead-letter queue if its queue has one. Its inbox row stays *failed*, and the idempotency guard rejects every later delivery of it again, without running the handler. Retrying it is a manual decision, done in this order:

1. Set the inbox row back to *waiting* with zero attempts.
2. Replay the message from the dead-letter queue.

```sql
-- forgo-adapter's inbox_events table. Narrow it down with message_id to retry single messages.
-- PostgreSQL
UPDATE inbox_events
SET status = 0, attempts = 0, updated_at = CURRENT_TIMESTAMP -- 0: InboxWaiting
WHERE subscriber_name = 'billing'                             -- SubscriberConfig.Name
  AND status = 2;                                             -- 2: InboxFailed

-- MySQL
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
UPDATE inbox_events FORCE INDEX (inbox_events_cleanup)
SET status = 0, attempts = 0, updated_at = UTC_TIMESTAMP(6)
WHERE subscriber_name = 'billing'
  AND status = 2;
```

On MySQL, the index hint and `READ COMMITTED` limit the update's locks to the rows it retries. Without the hint, MySQL scans the whole table once failed rows are a large share of it. Under `REPEATABLE READ`, the update also locks the gaps around the failed rows, or the whole table when it scans it, and consumers that complete or fail a message wait until it commits.

Do not reverse the order: a message replayed onto a row that is still *failed* is rejected again, straight back to the dead-letter queue. Reset `attempts` as well as `status`. Otherwise the replay exceeds `MaxAttempt` and fails at once with `msg_attempts`, without running the handler. *Rejected* messages (status 3, payloads that failed to bind) are retried the same way once the binding is fixed.

### Producer (outbox relay)

Save outgoing messages to the outbox in the **same transaction** as the state change that produced them. forgo-adapter's SQL outboxes do it with [`Save`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#inbox-and-outbox), which joins the transaction of its context:

```go
func (s *OrderService) Place(ctx context.Context, req PlaceReq) (PlaceResp, error) {
	order := newOrder(req)
	payload, err := json.Marshal(OrderPlaced{OrderID: order.ID})
	if err != nil {
		return PlaceResp{}, err
	}
	msg := worker.NewMessage("order.placed", "order.placed", "orders", payload) // the key routes it on RabbitMQ

	err = s.tx.Do(ctx, func(ctx context.Context) error {
		if err := s.orders.Insert(ctx, order); err != nil {
			return err
		}
		return s.outbox.Save(ctx, msg) // outbox.NewPostgres, in the transaction of ctx
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
| `ClaimTTL` | `1m` | How long a claim belongs to one producer instance. Older claims can be taken over, for example after a crash. A batch stops publishing at half its `ClaimTTL`, whatever the publishers' `Timeout`. Messages not published by then are released for the next batch, so another instance never takes over a claim that is still being published. |
| `BacklogInterval` | `15s` | How often the backlog of the outbox is sampled for an observer, when the storage reports it. See [Signals outside operations](#signals-outside-operations). |
| `BacklogLimit` | `10000` | The most pending messages a backlog sample counts, so that samples stay cheap however far the relay is behind. A sample at the limit means at least that many. |
| `DefaultPublisher` | name `default`, concurrency `1`, timeout `3s` | Publishes messages whose `Name` matches no entry in `Publishers`. Its broker publishers come from `IBroker.NewDefaultPublisher`; `Name` only labels its logs. |
| `Publishers` | | Dedicated publishers. A message goes to the publisher whose `Name` equals `Message.Name`; the same name is passed to `IBroker.NewPublisher`. `Concurrency` is the number of broker publishers, `Timeout` the per-publish timeout. |
| `ErrorPercentage`, `OnErrorPercentage` | off | When a batch's failure rate reaches `ErrorPercentage`, `OnErrorPercentage` is called, on the relay's goroutine, so it must return quickly. Without a callback, the breach is logged with `err_pct`. For metrics, a `worker.IRelayObserver` is told the counts of every batch (see [Signals outside operations](#signals-outside-operations)). |
| `Clean` | see below | Deletion of processed outbox rows. |

`New` rejects an empty or duplicate name in `Publishers`, and a negative `Limit`, `Interval`, `ClaimTTL`, publisher `Concurrency` or `Timeout`, `Clean.CleanupLimit` or `Clean.CleanupInterval`, which would otherwise only fail while serving.

**Required dependencies:** `worker.IBroker` and `worker.OutboxStorage`.

Each cycle **claims** up to `Limit` unprocessed messages, publishes them in parallel, then **completes** the claim. Published messages are marked processed, and failed ones are released for the next cycle. A failed publish is logged (`publish failed`) with the message's `msg_id`. Delivery is *at least once*; the consumer's inbox makes handling idempotent. When `Claim` or `Complete` fails, the batch is logged (`outbox batch failed`) and retried after `Interval`, so a storage outage pauses the relay instead of stopping it.

The relay does not keep the order of messages, whatever their `Key`. Messages are published in parallel, and a message whose publish failed is published in a later batch, after the ones that followed it.

**Shutdown.** Terminating the producer stops claiming, and lets the batch in flight publish and complete its claim. When the server's `TermTimeout` runs out first, `App` kills the server, which cancels the publishes in flight. The batch then completes its claim, which gives the messages it did not publish back to the outbox.

### Cleanup

Both servers periodically delete old rows:

```go
type CleanConfig struct {
	MaxTTL          time.Duration // age before deletion; default 30 days; negative disables
	CleanupLimit    int           // rows per delete batch; default 100
	CleanupInterval time.Duration // default 10 minutes
}
```

The consumer deletes *completed* and *rejected* inbox rows; *failed* rows are kept for inspection. The producer deletes processed outbox rows. A failed deletion is logged (`cleanup failed`) and retried at the next interval.

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

// Optional, for metrics: an outbox that also reports its backlog (see Signals outside operations).
type OutboxBacklogStorage interface {
	worker.OutboxStorage
	// The number of rows with processed_at IS NULL, counting no more than limit, and the created_at
	// of the first one by id. Sampled every ProducerConfig.BacklogInterval on every instance with
	// ProducerConfig.BacklogLimit, so it must be cheap.
	Backlog(ctx context.Context, limit int) (worker.OutboxBacklog, error)
}

type InboxStorage interface {
	// The status of the message for the subscriber, and false when it has no row.
	Status(ctx context.Context, messageID, subscriberName string) (worker.InboxStatus, bool, error)
	// Count an attempt of a waiting message and return the attempts so far. The first one
	// creates the row, waiting, with 1 attempt. Fail when the message is no longer waiting.
	// Called outside the handler's transaction, so that an attempt that rolled back still counts.
	IncrementAttempt(ctx context.Context, messageID, subscriberName string) (int, error)
	// Move the message from waiting to completed, joining the transaction in ctx. Fail when it is
	// no longer waiting, so that a duplicate handled at the same time rolls back.
	MarkCompleted(ctx context.Context, messageID, subscriberName string) error
	// Move the message from waiting to failed or rejected.
	MarkFailed(ctx context.Context, messageID, subscriberName string) error
	MarkRejected(ctx context.Context, messageID, subscriberName string) error
	// Delete completed and rejected rows updated before before, at most limit. Keep failed ones.
	DeleteBefore(ctx context.Context, before time.Time, limit int) (int64, error)
}
```

`InboxStatus` values are `InboxWaiting` (0), `InboxCompleted` (1), `InboxFailed` (2) and `InboxRejected` (3).

The SQL outbox and inbox storages of [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#inbox-and-outbox) (`inbox.NewPostgres`, `inbox.NewMySQL`, `outbox.NewPostgres`, `outbox.NewMySQL`) work on these tables:

```sql
-- PostgreSQL
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
CREATE INDEX outbox_events_pending ON outbox_events (id) WHERE processed_at IS NULL;
CREATE INDEX outbox_events_claim ON outbox_events (processing_id) WHERE processing_id IS NOT NULL;
CREATE INDEX outbox_events_processed ON outbox_events (processed_at, id) WHERE processed_at IS NOT NULL;

CREATE TABLE inbox_events (
    message_id      TEXT        NOT NULL,
    subscriber_name TEXT        NOT NULL,
    status          SMALLINT    NOT NULL,
    attempts        INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (message_id, subscriber_name)
);
CREATE INDEX inbox_events_cleanup ON inbox_events (status, updated_at);

-- MySQL 8
CREATE TABLE outbox_events (
    id            VARCHAR(255) NOT NULL PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    payload       JSON         NOT NULL,
    headers       JSON,
    created_at    DATETIME(6)  NOT NULL DEFAULT (UTC_TIMESTAMP(6)),
    message_key   VARCHAR(255) NOT NULL DEFAULT '',
    destination   VARCHAR(255) NOT NULL DEFAULT '',
    processing_id VARCHAR(255),
    processing_at DATETIME(6),
    processed_at  DATETIME(6),
    INDEX outbox_events_processed (processed_at, id),
    INDEX outbox_events_claim (processing_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE inbox_events (
    message_id      VARCHAR(255) NOT NULL,
    subscriber_name VARCHAR(255) NOT NULL,
    status          TINYINT      NOT NULL,
    attempts        INT          NOT NULL,
    created_at      DATETIME(6)  NOT NULL,
    updated_at      DATETIME(6)  NOT NULL,
    PRIMARY KEY (message_id, subscriber_name),
    INDEX inbox_events_cleanup (status, updated_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;
```

Every statement of the storages reads the primary key or one of these indexes, in the order it needs, and stops at its limit, so none slows down as rows pile up:

| Index | Serves |
| --- | --- |
| `outbox_events_pending` (PostgreSQL), `outbox_events_processed` (MySQL) | `Claim`, which reads unprocessed rows in `id` order. PostgreSQL does not treat `IS NULL` as an equality, so it needs the partial index to read them in that order. Without it, it walks the primary key past every processed row. |
| `outbox_events_claim` | `Complete`, which releases the rest of a claim by `processing_id` alone. Without it, every batch scans the backlog, which is largest while the broker is down, or on MySQL the whole table. |
| `outbox_events_processed` | The outbox cleanup, which deletes processed rows in `processed_at, id` order. |
| `inbox_events_cleanup` | The inbox cleanup, which deletes completed and rejected rows in `status, updated_at` order, so that it never reads the failed rows, which are kept. Queries for failed rows by `status` use it too, such as [retrying failed messages](#retrying-failed-messages), which forces it with an index hint on MySQL. |

The partial indexes' predicates hold whatever the parameters, so PostgreSQL uses them in generic plans too. On MySQL (8.0.17 or later), `utf8mb4_0900_bin` compares IDs and names byte for byte, as Go does. Under the default, case-insensitive collation, two messages whose IDs differ only in case share an inbox row, and the second is dropped as a duplicate. The MySQL storages begin their own transactions at `READ COMMITTED`, so that InnoDB's gap locks do not block the application's outbox inserts. That needs `binlog_format` `ROW`, the default, or `MIXED`. The `DATETIME` columns hold UTC: the storages stamp them with `UTC_TIMESTAMP(6)`, whatever the session time zone. The driver reads `created_at` in its `loc`, UTC by default. forgo-adapter's `Save` writes `CreatedAt` through the driver, so in that `loc` too, and stamps a message without one with `UTC_TIMESTAMP(6)`. An application that inserts rows itself writes `created_at` in UTC, or leaves it to the default.

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
	// most cs.Concurrency in flight. When ctx ends, stop, wait for every started
	// handler and return nil. Return an error when consuming stops on its own.
	Consume(ctx context.Context, cs ConsumerState) error
	Close() error
}

type IDelivery interface {
	Message() Message
	Complete(ctx context.Context) error // ack
	// attempt is the failed attempt (0 before it was counted): the broker derives its retry delay from it.
	Retry(ctx context.Context, attempt int) error // nack, redeliver
	Reject(ctx context.Context) error             // drop, no redelivery
}
```

`cs.Handler` handles a delivery to its end and settles it before it returns. The context you pass it can carry values of the broker, but ending it does not stop the handling: once received, a message is handled on a context that only killing the server cancels. `NewMessageFromDelivery` rebuilds a message from the delivery's headers. When the `x-msg-id` header is missing, set it to an ID the broker keeps across redeliveries, such as the broker's own message ID. Otherwise the message gets a new ID on every delivery, and the inbox neither counts its attempts nor recognizes it.

`process.ConcurrentRunner` handles the concurrency limit and the wait in `Consume`. forgo-adapter's [`rabbitmq`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#rabbitmq), [`pubsub`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#pubsub) and [`redisstream`](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#redisstream) brokers are complete implementations to start from.

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
	Terminate(ctx context.Context) error // graceful stop; ctx carries the server logger, expires after TermTimeout
	Kill()                               // forced stop when Terminate failed or did not return within TermTimeout
}
```

Contract:

- Export only the config. `New` returns a `forgo.IServer`, so the server type can stay unexported, like those of the built-in servers.
- `New` is the composition root. Resolve handlers and middlewares through the `Resolver` here, because it is sealed before `Serve`.
- `Init` builds what `Serve` needs. It does not block and starts no goroutines: when a later server fails to start, the app returns without terminating the servers it initialized. Store `ctx` to keep the logger and trace fields.
- `Serve` blocks. Returning an error stops the whole app, and `Run` returns it. Return nil after a graceful stop.
- `Terminate` makes `Serve` return and waits for in-flight work and the goroutines the server started, respecting `ctx`. `Terminate` may run before `Serve` started, when the app stops right after starting. The app counts the server as terminated once both returned.
- `Kill` force-stops what is left. The app calls it only when the server could not stop gracefully: `Terminate` returned an error, or `Terminate` and `Serve` did not both return within `TermTimeout`. A `Terminate` that gives up when `ctx` ends returns `ctx`'s error and leaves the rest to `Kill`. The app does not wait for the server after it.
- Run work as operations through `operation.Chain`, so every forgo middleware works on your server.
- Resolve the optional `operation.IObserver` in `New`, ignoring `forgo.ErrMissingDependency`, and put `middleware.Observe` directly inside `Recorder`, so your operations are measured like those of the built-in servers.
- A server that answers clients on a listener can also implement `ServeListener(ln net.Listener) error` (`forgotest.IListenerServer`): serve on `ln` as `Serve` does on its own listener, and close `ln`. [`forgotest`](#testing) then serves it in tests on a loopback listener. Make `Serve` listen on its address and call `ServeListener`, as the HTTP and gRPC servers do, so tests run the same code.
- A server with an `http.Handler` router can also implement `Handler() http.Handler` (`forgotest.IHandlerServer`). It returns the router with every route's chain, giving each request the values of the server's context, such as its logger. Tests can then call it in process, as with the HTTP server.
- A server that runs work on its own, which `forgotest` never serves, can let tests run it one operation at a time:
  - **Jobs:** `Jobs() map[string]func(ctx context.Context) error` (`forgotest.IJobServer`).
  - **Message handling:** `Subscribers() map[string]func(ctx context.Context, d worker.IDelivery) error` (`forgotest.IConsumerServer`).

  Each function runs the closure the server's own loop runs, through the chain, and returns the error of the chain. Its context ends with `ctx` and adds `ctx`'s values to those of the server's context.

A complete example: a server that runs a task at a fixed interval.

```go
package ticker

import (
	"context"
	"errors"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/middleware"
	"github.com/efegreat/forgo/operation"
	"github.com/efegreat/forgo/types"
)

const ServerName = "ticker"

type Task interface {
	Run(ctx context.Context) error
}

// Config is the ServerFactory. Embedding forgo.ServerConfig adds
// Name, Term, Middlewares and DisableBaseMiddlewares.
type Config struct {
	forgo.ServerConfig
	TaskName string
	Interval time.Duration
	Task     any // constructor returning a Task, resolved by bootstrap
}

func (c Config) Name() string { return c.ServerConfig.NameOr(ServerName) }

func (c Config) New(r forgo.Resolver) (forgo.IServer, error) {
	sc, err := c.ServerConfig.Sanitize().ResolveMiddlewares(r)
	if err != nil {
		return nil, err
	}
	task, err := r.Call[Task](c.Task)
	if err != nil {
		return nil, err
	}
	observer, err := r.Resolve[operation.IObserver]()
	if err != nil && !errors.Is(err, forgo.ErrMissingDependency) {
		return nil, err
	}
	c.ServerConfig = sc
	return &server{cfg: c, task: task, observer: observer}, nil
}

type server struct {
	cfg      Config
	task     Task
	observer operation.IObserver // nil without one
	ctx      context.Context
	handler  operation.Handler
	stop     chan struct{}
	done     chan struct{}
}

func (s *server) Name() string           { return s.cfg.Name() }
func (s *server) Term() forgo.TermConfig { return s.cfg.Term }

func (s *server) Init(ctx context.Context) error {
	s.ctx = ctx
	s.stop, s.done = make(chan struct{}), make(chan struct{})

	var mws []operation.Middleware
	if !s.cfg.DisableBaseMiddlewares {
		mws = append(mws,
			middleware.ID(),
			middleware.Logger(nil),
			middleware.Recorder(nil, nil, nil),
			middleware.Observe(s.observer),
			middleware.Recovery(nil),
		)
	}
	mws = append(mws, s.cfg.ResolvedMiddlewares()...)

	s.handler = operation.Chain(func(ctx *operation.Context) error {
		return s.task.Run(ctx)
	}, mws...)
	return nil
}

func (s *server) Serve() error {
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
				Args:    new(types.Map[any, any]),
			})
		}
	}
}

func (s *server) Terminate(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *server) Kill() {}
```

```go
forgo.WithServers(ticker.Config{TaskName: "heartbeat", Interval: 30 * time.Second, Task: NewHeartbeat})
```

`ServerConfig` helpers:

| Method | Description |
| --- | --- |
| `NameOr(name) string` | `Name`, or `name`, the server's default name, when `Name` is empty. A config's `Name()` returns it. |
| `Sanitize() ServerConfig` | Applies defaults (`TermTimeout` of zero or less → 30s). |
| `ResolveMiddlewares(r) (ServerConfig, error)` | Resolves `Middlewares` and returns a copy that holds them. |
| `ResolvedMiddlewares() []operation.Middleware` | The resolved server middlewares. |

`forgo.ResolveMiddlewares(r, factories)` resolves any other list of `MiddlewareFactory`, such as route-level middlewares. It fails on a nil factory, and on a factory that returns a nil middleware, so neither reaches a chain.

---

## CLI

The CLI runs **one command per invocation**, with the same resources, providers and middleware model as an App. Where an App runs servers until it is stopped, a CLI bootstraps, runs a single command as an operation, waits for its background goroutines, and returns.

```go
func main() {
	c := cli.New(
		cli.WithName("shop-cli"),
		cli.WithLogger(config.Logger),
		cli.WithResources(config.DB()),
		cli.WithProviders(config.Adapters...),
		cli.WithProviders(config.Services...),
		cli.WithConfig(cli.Config{
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewAuditMiddleware)},
			Commands: []cli.CommandConfig{
				{Name: "migrate", Command: migration.Factory(migration.CommandConfig{})},
				{Name: "openapi", Command: openapi.Factory(openapi.CommandConfig{HTTP: config.HTTP})},
				{
					Name:    "reindex",
					Lock:    operation.Lock{Key: "cmd:reindex", TTL: time.Hour},
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

1. Creates the logger with `app_name`. It writes to stderr unless `logger.Config.Output` is set, since commands print their results to stdout, and it is the default logger (`logger.SetDefault`) until `Run` returns.
2. Checks the configuration: at least one command, and every command has a unique, non-empty `Name` and a `Command`. A misconfiguration fails before anything is bootstrapped.
3. Looks up the command named by the first argument of the command line (`os.Args[1]`, or `cli.WithArgs`). A missing or unknown name returns an error that wraps `cli.ErrNoCommand` or `cli.ErrUnknownCommand` and lists the available commands in declaration order, before anything is bootstrapped.
4. Builds the bootstrap. Every resource and provider is created, even for commands that don't use them.
5. Resolves the optional `operation.ILocker` and `operation.IObserver`, the global and command middlewares, and the command constructor. Then seals the bootstrap.
6. Runs the command as an operation (`Type: "command"`, `Name: <command>`) through `ID` → `Logger` → `Locker` → `Recorder` → `Observe` → `Recovery`, then the global middlewares, then the command middlewares.
7. Inside the chain: creates the command from its factory, calls `Init` with a fresh `flag.FlagSet` named after the command, parses the arguments after the command name, then calls `Validate` and `Run`. `-h` and `-help` print the command's flags to stderr and end the run without an error, before `Validate`. Parse failures carry code `command_parse` and message `invalid arguments`, with the parse error as the cause, so they print as `invalid arguments: …`. Validation failures carry code `command_validate` and message `invalid command`, and print as `invalid command: …`.
8. Waits for goroutines started with `process.Go`, closes resources, and returns the command's error, joined with any error from closing resources.

The command's context is cancelled on SIGINT or SIGTERM, so a long-running command can stop cleanly. A second signal terminates the process immediately.

`Run` returns the error instead of exiting, so `main` chooses the exit code after cleanup. Failures before the command runs are also logged, with `err_severity`.

`CommandConfig.Lock` keeps a command from running concurrently across machines. It needs an `operation.ILocker` in the bootstrap; without one, `Run` fails before the command starts. While another run holds the lock, `Run` returns an error with code `command_locked` without running the command, so scripts can tell that it did not run. With `DisableBaseMiddlewares`, locks are not applied.

### Testing a command

`cli.WithArgs` replaces `os.Args`, so a test can run a command through the whole CLI, with its middlewares and lock:

```go
err := cli.New(
	cli.WithArgs([]string{"shop-cli", "reindex", "-index=products"}),
	cli.WithProviders(NewFakeSearchService),
	cli.WithConfig(cfg),
).Run()
```

---

## Built-in commands

### `migrate`: SQL migrations

Package `github.com/efegreat/forgo/commands/migration` runs SQL migrations on PostgreSQL and MySQL. It uses [sql-migrate](https://github.com/rubenv/sql-migrate) file annotations and sources, with its own context-aware planner and executor.

**Default command**, which needs exactly one `*sql.DB` in the bootstrap:

```go
{Name: "migrate", Command: migration.Factory(migration.CommandConfig{})}
```

It detects the dialect from the driver (`github.com/lib/pq` and pgx's `stdlib` → postgres, `github.com/go-sql-driver/mysql` → mysql), reads `./migrations`, tracks applied migrations in the table `migrations`, and takes an advisory lock so concurrent runners serialize. The package imports no driver: import yours in `main`.

**Configured command.** Every field of `CommandConfig` is optional:

```go
//go:embed migrations/*.sql
var migrationFiles embed.FS

{Name: "migrate", Command: migration.Factory(migration.CommandConfig{
	Database: "primary",
	FS:       migrationFiles,
	Config: migration.Config{
		Table:   "schema_migrations",
		Timeout: 10 * time.Minute,
	},
})}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `Database` | the only `*sql.DB` | Name of the `*sql.DB` resource to migrate |
| `Dir` | `migrations` | Directory of the migration files: inside `FS` when set, otherwise on disk relative to the working directory. `new` always writes here on disk. |
| `FS` | none (read from disk) | File system holding `Dir`, such as an `embed.FS`, so the binary does not need the files on disk |
| `Config.Dialect` | detected from the driver | `postgres` or `mysql`. Set it when the driver is wrapped (for example for tracing) or not one of the above. |
| `Config.Table` | `migrations` | Tracking table, created by the first `up` or `down` |
| `Config.Schema` | the connection's | Schema (PostgreSQL) or database (MySQL) of the tracking table. It must exist. |
| `Config.Timeout` | none | Bounds each run, including the wait for the lock |
| `Locker` | a lock shared by every forgo migrator | `migration.NewPostgresLocker(key)` or `migration.NewMySQLLocker(name)` |
| `Output` | `os.Stdout` | Where results and progress are printed |

`Factory` resolves the database and builds the migrator when the CLI resolves the command, so a bad configuration fails before the command runs.

**Programmatic use.** `migration.New(db, source, locker, cfg)` returns a `*Migrator` with `Up`, `Down`, `Plan` and `Status`, for example to migrate a test database in `TestMain`. A nil locker and an empty `Config` get the same defaults as the command:

```go
m, err := migration.New(db, &sqlmigrate.FileMigrationSource{Dir: "../migrations"}, nil, migration.Config{})
if err != nil {
	return err
}
_, err = m.Up(ctx, 0) // 0 = all pending
```

A failed migration returns a `*migration.ExecutionError` naming the migration and the statement. `migration.ErrUnknownMigrations` and `migration.ErrUnsupportedDialect` can be matched with `errors.Is`. `migration.WithProgress` attaches callbacks that `Up` and `Down` call when they wait for the lock and before and after each migration.

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

`up` and `down` print each migration as it runs:

```console
$ app migrate -a=up
applying 20261004003151237_create_users.sql
applied 20261004003151237_create_users.sql (42ms)
up: 1 migration completed
```

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

- `up` and `down` take the advisory lock on one pinned connection. All SQL, tracking writes and the lock share that session. While another run holds the lock, they print `waiting for the migration lock` and keep trying, up to `Timeout`. The tracking table is created on first use.
- Each migration runs in its own transaction, including its tracking row, unless it is marked `notransaction`. PostgreSQL DDL is transactional. MySQL DDL may commit implicitly.
- `up` applies pending migrations in source order, including one that sorts before applied ones, such as a migration merged later from a branch. `down` rolls back applied migrations in reverse source order, not in reverse order of application.
- `status` and `plan` are read-only. They take no lock and create no table.
- Applied migrations missing from the source show as `unknown` in `status`, and block `up`, `down` and `plan`.
- The first SIGINT or SIGTERM stops `up` and `down` after the running migration, which completes with its tracking row, and prints `stop requested`. A second signal kills the process. `Timeout` interrupts the running migration itself.
- Treat applied files as immutable; there are no checksums.
- PostgreSQL advisory locks need a direct connection or session pooling, not transaction pooling. The default MySQL lock is server-wide, so migrations of different databases on one server take turns; give each app its own `NewMySQLLocker` name to avoid that.
- MySQL does not need `parseTime=true`. Without it, `status` reads `applied_at` as UTC, so keep the DSN's `loc` at its default, UTC.
- On failure the error names the migration and the statement number. Migrations completed before the failure stay applied and are reported.

### `openapi`: API documents

Package `github.com/efegreat/forgo/commands/openapi` generates **OpenAPI 3.1** documents straight from your `http.Config`. It never starts a server or calls handlers.

```go
{Name: "openapi", Command: openapi.Factory(openapi.CommandConfig{HTTP: config.HTTP})}
```

`HTTP` is the `http.Config` the server uses. `Options` configure the generator:

```go
{Name: "openapi", Command: openapi.Factory(openapi.CommandConfig{
	HTTP: config.HTTP,
	Options: []openapi.Option{
		openapi.WithInfo(openapi.Info{
			Title:       "Shop API",
			Description: "Public HTTP API of the shop",
			Version:     "1.4.0", // the version of the unversioned document
		}),
		openapi.WithSchema[shop.Money](openapi.Schema{"type": "string", "format": "decimal"}),
		openapi.WithDocument(func(version string, doc *openapi.Document) error {
			doc.Servers = []openapi.Server{{URL: "https://api.example.com"}}
			return nil
		}),
	},
})}
```

`Factory` builds the generator when the CLI resolves the command, and resolves no dependency.

| Option | Default | Effect |
| --- | --- | --- |
| `WithInfo(Info)` | `API Doc`, `HTTP API Doc`, `1.0.0` | Title, description, and the version of the document of unversioned routes. Versioned documents take the route version. An empty title or version gets the default. |
| `WithSchema[T](Schema)` | | Documents `T` as the given schema in bodies, responses and parameters. Use it for types whose JSON shape cannot be inferred (their own `json.Marshaler`/`json.Unmarshaler`) and to give named types a `format` or `enum`. `validate` constraints still apply around it. |
| `WithDocument(hook)` | | Runs `hook(version, doc)` on every generated document, in version order, to add what routes do not declare: `Servers`, `Security`, `Components.SecuritySchemes` for authentication no [middleware contract](#middleware-contracts) can declare, such as OAuth 2 scopes, `Tags`, and per operation (found with `doc.Operation(routeName)`) `Tags`, `Summary`, `Deprecated` or `Security`. An empty, non-nil `Operation.Security` documents an operation without authentication. A hook error fails generation. |

```console
$ app openapi -o=./docs/openapi.json
$ app openapi -o=./docs/openapi.yaml
$ app openapi -output=./docs/api.yml -format=yaml
```

**One document per route version.** The version is appended to the output name: `-o=./docs/openapi.json` with versions `v1` and `v2` writes `./docs/openapi-v1.json` and `./docs/openapi-v2.json`. Each document contains only its version's routes and schemas, and `info.version` is the route version, or `Info.Version` for unversioned routes. `-format` defaults to the output extension (`.yaml`/`.yml` → YAML, otherwise JSON). Positional arguments are rejected. The parent directory must exist, and an existing output must be a regular file, not a symlink.

**Writing.** All documents are generated and encoded before any file is written, so a generation error leaves every file untouched. Each file is replaced atomically, but not together with the others: if writing one fails, the files written before it stay replaced.

**Private routes.** Routes with `DisableAPIDoc` are left out, such as the [live config admin endpoints](#admin-endpoints). A version whose routes all have it gets no document.

**Paths and methods.** Paths are checked against `ServeMux` first, so a path the server cannot register fails with ServeMux's error. `{name...}` is documented as `{name}`, described as matching the rest of the path, and `{$}` is dropped (`/users/{$}` → `/users/`), because OpenAPI has neither. The method must be one OpenAPI describes, in upper case, since ServeMux methods are case-sensitive. A route with an empty method matches every method and needs `DisableAPIDoc`. Errors name routes as `route "name" (METHOD /path)`, or `route METHOD /path` without a name.

**What is documented, for each route:**

- `operationId` from `Name`, the description from `Description`, and the path including the version prefix.
- **Parameters and body** from the request type, for every source a field is bound from:
  - `uri` fields become required path parameters. Every `uri` field must match a path wildcard, and every wildcard must be bound by a `uri` field of the request or of a middleware's `Params`. A `uri` field cannot be a slice or an array, because a wildcard binds one value.
  - `header` and `query` fields become parameters. Query arrays use `form`/`explode`. Header arrays take one header line per value, and say so.
  - `json` fields form the request body schema. Fields without a source tag are left out.
  - A field with several sources (`query:"locale" json:"locale"`) is documented in each, with a description naming the others and the one that wins when a request sends several: header, then path, then query, then body. None of its copies is required, except a path parameter.
  - Parameters are never `null`: a pointer makes a parameter optional. A parameter's type is a primitive, `time.Time`, an `encoding.TextUnmarshaler`, a type with `WithSchema`, or a slice or array of these.
  - `"-"` disables a source.
- **Middleware parameters** from the `Params` of every server and route middleware attached with `http.MiddlewareAs` or `http.WithContract` (see [Middlewares that read request parameters](#middlewares-that-read-request-parameters)):
  - `Params` takes `header`, `query` and `uri` fields only. A `json` field fails generation, because the body belongs to the handler.
  - A `uri` field must exist in the path of every route the middleware runs on.
  - A parameter declared by several middlewares or by the handler is documented once, as required when any declaration requires it. Declarations with different schemas fail generation.
  - To read [middleware contracts](#middleware-contracts), the generator calls each middleware factory with a resolver that provides nothing of the application. `http.MiddlewareAs` and `http.WithContract` factories report their contracts there without calling their constructor or factory, and `http.RateLimit`, `http.RateLimitWith` and `http.JWTAuth` report theirs without a limiter. Other factories are ignored: they fail to resolve their dependencies, or construct a middleware that is discarded.
- **The `X-Request-ID` header**, which the server takes the operation ID from, as an optional header parameter of every operation.
- **Authentication** from the `Auth` of middleware contracts: the operation's `security` lists every credential a middleware accepts, and no credential at all when `Optional` is set, and their schemes are added to `components.securitySchemes`. `http.JWTAuth` accepts a bearer token (the `bearerAuth` scheme, `bearerFormat: JWT`) and its `Cookie` when one is set. Basic authentication is the `basicAuth` scheme, and a key in a header, query parameter or cookie an `apiKey` scheme named after it, with characters other than letters, digits, `.`, `-` and `_` replaced by `_`. A route behind several authenticating middlewares needs a credential for each. Two different schemes with one name fail generation, such as a cookie named `bearerAuth`.
- **Middleware responses** from the `Rejections` and `Headers` of middleware contracts: each rejection with the body your serializer writes for its error, at the status it writes, such as the `401` of `http.JWTAuth` with `WWW-Authenticate` and the `429` of `http.RateLimit`/`RateLimitWith` with `Retry-After`. A response carries the headers of the middlewares that ran before it. A middleware's `Headers`, such as the `RateLimit-*` headers unless `DisableHeaders` is set, are also on the success and `default` responses of every route it runs on. Failures of the middlewares' own, such as `auth_unavailable` or `rate_limit_unavailable`, are left to the `default` response.
- **Validation hints** from `validate` tags: `required`, `min`, `max`, `len`, `gt`, `gte`, `lt`, `lte`, `oneof`, `email`, `uuid*`, `hostname`, `hostname_rfc1123`, `url`, `uri`, `ip`, `ipv4` and `ipv6` become schema constraints. Rules after `dive` or `keys` are not applied to the collection itself.
- **Response schema** from the response type, placed inside your actual serializer's envelope. The generator probes the configured `Serializer` (default or custom), finds where the result lands, and copies the status, media type and response headers it observes.
- A `default` error response, from probing the serializer's `Error`.
- `EmptyHandler` and `HealthHandler` routes have no request, other than `X-Request-ID`, and no result.
- `LiveConfigsHandler` routes have a result whose config values have no schema. `SetLiveConfigHandler` routes have their `name` and `If-Match` parameters, but no body, because it depends on the config.

**Supported types:** primitives, pointers (nullable), slices, arrays, maps with JSON-compatible keys, nested and recursive structs (as `components/schemas`), embedded structs with JSON dominance rules, `time.Time`, `json.Number`, `json.RawMessage`, `types.JSONMap[V]` (an object of `V` values), and text types: `encoding.TextUnmarshaler` in requests and `encoding.TextMarshaler` in responses. A type with its own JSON encoding (`json.Unmarshaler` in a request, `json.Marshaler` in a response) fails generation, because its shape cannot be inferred, unless it has a `WithSchema`.

**Component names** follow the Go type: `Product`, and `PageProduct` for `Page[shop.Product]`. The request schema of a type also used in a response, which keeps only `json` fields and carries validation, is `ProductRequest`. Colliding names get the package name as a prefix (`ShopProduct`), then a hash suffix if that is not enough. Names do not depend on route order.

**Programmatic use:**

```go
gen := openapi.New(config.HTTP, openapi.WithInfo(openapi.Info{Title: "Shop API"})) // New sanitizes the config
docs, err := gen.GenerateAll(ctx)                                                  // map[version]*openapi.Document
err = openapi.WriteFiles(ctx, "./docs/openapi.json", "json", docs)
data, err := openapi.Marshal(docs["v1"], "yaml")
```

`openapi.NewCommand(gen)` returns the command itself, for a `cli.CommandFactory` of your own.

---

## Testing

`github.com/efegreat/forgo/forgotest` runs the app that `main` runs, for feature tests (one endpoint) and E2E tests (a flow across endpoints), so tests need no app of their own.

### The app as a function

Build the app in a function that `main` and the tests both call. Read the configuration that tests change, such as a database address, inside functions too, not in package-level `var`s: Go evaluates those before `TestMain`, so the address of a test container could never reach them.

```go
// config/config.go
func DB() sqldb.Config {
	return sqldb.Config{Name: "main", DriverName: "postgres", DSN: forgo.Env("DATABASE_URL", "")}
}

// internal/app/app.go
func New(servers ...forgo.ServerFactory) *forgo.App {
	return forgo.NewApp(
		forgo.WithName("shop"),
		forgo.WithResources(config.DB()),
		forgo.WithProviders(Providers...),
		forgo.WithServers(servers...),
	)
}

// cmd/http/main.go
func main() {
	if app.New(config.HTTP).Run() != nil {
		os.Exit(1)
	}
}
```

### One env per package

Build one `forgotest.Env` per test package, in `TestMain`, and close it once the tests ran:

```go
// test/http/main_test.go
var env *forgotest.Env

func TestMain(m *testing.M) {
	// start the package's database container and set DATABASE_URL first
	var err error
	if env, err = forgotest.New(app.New(config.HTTP)); err != nil {
		log.Fatal(err)
	}
	defer env.Close()
	m.Run()
}

func TestHealth(t *testing.T) {
	addr, err := env.Addr(http.ServerNameHTTP)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := nethttp.Get("http://" + addr + "/health")
	// ...
}
```

The tests of a package share the env and what it is connected to, such as the database. Each test resets the state it depends on, for example with SQL fixtures, and the tests do not call `t.Parallel()`. Packages still run in parallel, each in its own process.

### Live server or router

An env serves the HTTP server two ways at once, and each test picks one:

```go
// live: a real server on a loopback port
addr, err := env.Addr(http.ServerNameHTTP)
resp, err := nethttp.Post("http://"+addr+"/v1/users", "application/json", body)

// router: the routes in process, without the network
h, err := env.Handler(http.ServerNameHTTP)
rec := httptest.NewRecorder()
h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/users", body))
```

Both run the same server instance, so they share its resources. Both also run the route's whole chain: middlewares, binding, validation, the handler, error mapping and the request log. They differ below the router:

| | `env.Addr` (live) | `env.Handler` (router) |
| --- | --- | --- |
| Network | A TCP connection on `127.0.0.1` | None. A request costs about 2 µs instead of about 30 µs. |
| net/http's server | Runs: timeouts, `MaxHeaderBytes`, TLS, HTTP/2, `OPTIONS *`, connection observers | Does not run |
| Client | A real one, which encodes the request and reads the response as clients do | `httptest.NewRequest` and `httptest.NewRecorder` |
| Hijacking, such as WebSockets | Works | Fails, since `ResponseRecorder` cannot hijack |
| Request context | net/http's, derived from the server's context | The request's own, which also gets the values of the server's context, such as its logger |

The gRPC server has no router to call in process: grpc-go only serves calls through its HTTP/2 transport. Test it through `env.Addr`.

### Cron jobs and message handlers

An env never serves cron, the consumer or the producer. Tests run their operations one at a time instead, synchronously:

```go
// a cron job, once, through its chain: Locker, Recorder, Tolerance, Recovery and middlewares
err := env.RunJob(t.Context(), "expire_sessions")

// a message, to a subscriber, as the broker would deliver it
got, err := env.Deliver(t.Context(), "shipping", worker.NewMessage("order_created", "", "orders", payload))
// got.Settlement: forgotest.Completed, Retried (got.Attempt) or Rejected; got.Err: the error of the handling
```

- **`env.RunJob(ctx, name)`** runs the job once, as its schedule would, and returns the error of its chain: what the job returned once the retries of its `Tolerance` ran out, or the `*middleware.PanicError` of a panic. A run whose `Lock` another holder has is skipped, and returns nil. Whether a valid schedule is the one you meant is configuration, not behavior, so tests never run the scheduler.
- **`env.Deliver(ctx, subscriber, msg)`** hands `msg` to the subscriber and returns how the consumer settled it once the handling ended. It runs what production runs:
  - the handler of the message's name, through its chain, or `msg_no_handler` for a name without one;
  - the binding and the validation of the payload;
  - the inbox and the transaction of the application;
  - the retry rules (`MaxAttempt`, `ErrNoRetry`).

  So a test catches a name or a payload that the publisher and the handler disagree on, and a message delivered twice is handled once. `msg` needs the ID of `worker.NewMessage`. `Delivery.Err` is the error of the handling, and the error `Deliver` returns is the env's own, such as `ErrNotFound` for an unknown subscriber.
- **Context:** in both, `ctx` ends the run or the handling, and adds its values to those of the server's context, such as its logger.
- **Lookup:** both find the job or the subscriber by name across the servers that implement `forgotest.IJobServer` or `forgotest.IConsumerServer`, with the errors of `Addr`.

### Brokers in tests

Tests run no broker: delivery through a broker depends on timing, and what a broker adds is the broker's own guarantee, which forgo-adapter's round-trip tests cover. The consumer and the producer still need an `IBroker` to be created, so replace the app's broker resource with the stand-in `forgotest.Broker`:

```go
env, err := forgotest.New(app.New(config.Consumer),
	forgotest.ReplaceResource[rabbitmq.Config](forgotest.Broker("rabbitmq")))
```

- **`forgotest.ReplaceResource[F](factory)`** puts `factory` in place of the app's one resource factory of type `F` before anything is built, so the replaced factory never runs and nothing connects.
  - `New` fails with `forgotest.ErrNotFound` when no resource factory has type `F`, and with `forgotest.ErrAmbiguous` when more than one has.
  - A provider that asks for the concrete type of the replaced resource's value, rather than an interface it implements such as `worker.IBroker`, fails in `New` with `forgo.ErrMissingDependency`. Depend on the interface. The value types of forgo-adapter's brokers are unexported, so providers can only ask for `worker.IBroker`.
- **`forgotest.Broker(name)`** is a resource named `name`, so `boot:"name"` tags still find it. Its value is a `worker.IBroker` whose publishers discard what they are given and whose subscribers never consume.

**Broker topology is the application's responsibility.** Application tests cover that a publishing operation saves the right message and that the handler handles it, but not the route between them: that the destination a publisher writes to is bound to the queue or subscription a subscriber reads, or that the dead-letter and retry queues exist. A wrong binding passes every test and loses messages in production. Verify the topology where it is declared, in staging or smoke checks.

### What tests replace

Tests should run as much of the production code as they can, so prefer configuration to replacement, and replace only what cannot run deterministically in a test:

| Dependency | Example | In tests |
| --- | --- | --- |
| Infrastructure that runs in a container | SQL databases, Redis | Nothing is replaced: the resource's configuration points at the container (see [The app as a function](#the-app-as-a-function)). |
| Broker | RabbitMQ, Pub/Sub, Redis Streams | The stand-in `forgotest.Broker`, through `ReplaceResource` (see [Brokers in tests](#brokers-in-tests)). |
| Third-party services | Payments, email, SMS, other companies' APIs | Preferably a fake `httptest.Server`, whose URL the client's configuration reads. Otherwise, a fake in place of the provider of the client, through `Replace`. |

```go
// test/support/env.go
var Gateway = &FakeGateway{} // the package's tests reset it in their fixtures

func HTTP() (*forgotest.Env, error) {
	return forgotest.New(app.New(config.HTTP),
		forgotest.ReplaceResource[rabbitmq.Config](forgotest.Broker("rabbitmq")),
		forgotest.Replace(func() payment.Gateway { return Gateway }))
}
```

- **`forgotest.Replace(providers...)`** puts each constructor in place of the app's provider whose declared result type is the same, before anything is built, so the replaced provider never runs. The replacement takes the replaced provider's position, so it may depend on what the providers before it provide, such as configuration.
- **The result types must be identical.** `func() *FakeGateway` replaces nothing in an app whose provider returns `payment.Gateway`: declare the provider's type, as `func() payment.Gateway { return Gateway }` does. So only a provider whose result is an interface takes a fake of another type. Have providers return the interfaces their users depend on.
- **Failures:** `New` fails with `forgotest.ErrNotFound` when no provider of the app has the result type of a replacement, and the error names the interfaces the replacement implements that a provider returns. It fails with `forgotest.ErrAmbiguous` when two replacements have the same result type.
- **Keep a handle on the fake.** An env resolves nothing for tests, just as the app resolves nothing once it started. So the test builds the fake, replaces the provider with a constructor that returns it, and keeps it, to set its answers and read what it was asked. The tests of a package share it, like the database, so each test resets it in its fixtures.

### Messages an operation publishes

An operation publishes a message by saving it to the outbox, in its transaction, and the producer's relay publishes it later. An env never serves the producer, so tests read the outbox instead:

```go
msgs, err := env.Outbox(t.Context())
```

- **`env.Outbox(ctx)`** returns the messages saved to the app's `worker.OutboxStorage` since the previous call, in claim order, as the relay would publish them. It reads them with `OutboxStorage.Claim`, as the relay does, so a test sees what the relay would get, and not what the operation meant to save.
  - Its claims have a zero `ExpiredBefore`, so they take only the messages no claim holds. The messages stay claimed and are never published, so the next call returns only the messages saved after it. A storage whose claims return a message again, which would make the relay publish it twice, fails `Outbox` with `forgotest.ErrClaimedTwice`.
  - The env finds the storage in `New`, as servers find their optional dependencies. An app without one, or with two, starts all the same, and `Outbox` fails with `forgo.ErrMissingDependency` or `forgo.ErrAmbiguousDependency`. No producer is needed: an HTTP app whose providers include the outbox storage reads it too.
- **The tests of a package share the outbox.** A message that a test saved but did not read comes out of the next test's `Outbox`, so reset the outbox table in each test's fixtures.

**The contract test.** Together, `Outbox` and `Deliver` test both ends of a message without a broker: the endpoint saves the message, and the subscriber's handler handles what was saved. Build one env per app, over the same database, just as two processes run in production:

```go
// test/contract/main_test.go
var api, consumer *forgotest.Env

func TestMain(m *testing.M) {
	// start the database container and set DATABASE_URL first
	broker := forgotest.ReplaceResource[rabbitmq.Config](forgotest.Broker("rabbitmq"))
	var err error
	if api, err = forgotest.New(app.New(config.HTTP), broker); err != nil {
		log.Fatal(err)
	}
	defer api.Close()
	if consumer, err = forgotest.New(app.New(config.Consumer), broker); err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	m.Run()
}

// test/contract/order_created_test.go
func TestOrderCreatedReachesShipping(t *testing.T) {
	// reset the orders, the outbox and the inbox with the test's fixtures first
	createOrder(t, api, `{"sku":"a-1","quantity":2}`) // POST /v1/orders, through api.Addr or api.Handler

	msgs, err := api.Outbox(t.Context())
	if err != nil || len(msgs) != 1 {
		t.Fatalf("outbox: %v, %v", msgs, err)
	}
	got, err := consumer.Deliver(t.Context(), "shipping", msgs[0])
	if err != nil || got.Settlement != forgotest.Completed {
		t.Fatalf("delivery: %+v, %v", got, err)
	}
	// check the shipment the handler created
}
```

The test catches a message name that the handler is not registered under, and a payload that the handler's validation rejects. What it cannot catch is the broker topology between the two, as [above](#brokers-in-tests): that the subscriber named in the test receives the publisher's destination in production.

### What an env does

- **`forgotest.New(app, opts...)`** does what `Run` does before serving:
  1. Builds the bootstrap of `app.Definition()`, with the replacements of `ReplaceResource` and `Replace`.
  2. Creates and initializes every server in order, with the `app_name`, `app_id`, `server_name` and `server_id` log fields.
  3. Seals the bootstrap.

  A configuration that fails the app's start therefore fails `New` the same way, and a failed `New` closes what it built. Unlike `Run`, it catches no signal and does not install its logger as the default one.
- **Servers that implement `forgotest.IListenerServer`,** the HTTP and gRPC servers, are served on a loopback listener of the env's own (`127.0.0.1:0`), whatever their `Addr`. Packages running in parallel never collide on ports.
- **Other servers,** such as cron and the worker's, are created and initialized but never served. Nothing runs on a schedule or consumes from a broker during tests.
- **`env.Addr(name)`** returns the `host:port` of the served server named `name`, such as `http.ServerNameHTTP` or `grpc.ServerNameGRPC`. It fails with `forgotest.ErrNotFound` when no served server has the name, and with `forgotest.ErrAmbiguous` when more than one has it, such as two HTTP servers without a [`ServerConfig.Name`](#running-several-servers-in-one-app) of their own.
- **`env.Handler(name)`** returns the router of a server that implements `forgotest.IHandlerServer`, the HTTP server, with the same errors.
- **`env.RunJob(ctx, name)`** and **`env.Deliver(ctx, subscriber, msg)`** run one operation of a server that implements `forgotest.IJobServer` (cron) or `forgotest.IConsumerServer` (the worker's consumer). See above.
- **`env.Outbox(ctx)`** returns what the operations saved to the outbox since the previous call, through the app's `worker.OutboxStorage`. See above.
- **`env.Close()`** does three things and returns the failures joined, including those of serving:
  1. Terminates the served servers in parallel. It signals each `TermSig` but does not sleep for `TermSig.Wait`, and it kills a server whose `Terminate` fails or outlasts its `TermTimeout`.
  2. Waits for the goroutines started with `process.Go`.
  3. Closes the resources in reverse order.

  Later calls return nil.
- **Logs** use the app's logger configuration, but go to `os.Stderr` and never to a file. `go test` with package arguments, such as `./...`, shows a passing package's output only with `-v`. `forgotest.WithLogger(cfg)` replaces the configuration, for example to raise the level, or to read the logs from a buffer through `Output`.

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
	Output:  nil, // os.Stdout, or os.Stderr in a CLI; a buffer in tests
	Console: logger.ConsoleConfig{Enabled: true, NoColor: false},
	File: logger.FileConfig{
		Enabled:    true,
		Filename:   "logs/app.log", // default "<program>.log" in the working directory
		MaxSize:    100,            // MB before rotation, default 100
		MaxBackups: 7,              // rotated files kept, default all
		MaxAge:     30,             // days, default forever
		Compress:   true,
	},
}
```

| Outputs enabled | Result |
| --- | --- |
| none | JSON lines to `Output` (production default) |
| `Console` | human-readable output to `Output` |
| `File` | rotating JSON files only |
| both | human-readable output to `Output`, and rotating JSON files |

`Output` defaults to stdout in an App, and to stderr in a CLI, whose commands print their results to stdout.

Without `MaxBackups` or `MaxAge`, rotated files are never deleted. A file must not be shared by two processes, since each rotates it on its own and lines are lost. The default name is that of the executable (without `.exe`), so a server and a CLI built from one project write to different files.

Timestamps are RFC 3339 with microseconds and the zone offset, for example `2026-10-07T23:17:54.168989+03:00`. Run the process with `TZ=UTC` for `Z` timestamps. Importing `logger` sets `zerolog.TimeFieldFormat` to this format; set it in `main` to use another.

A context without a logger, such as the `context.Background()` of a constructor, gets the **default logger**. `App.Run` and `CLI.Run` make theirs the default while they run (`logger.SetDefault`), so resources, providers and background code log with the configured outputs, level and `app_name`. Before that, the default writes JSON lines to stdout, at every level.

### Contextual fields

Each layer adds its fields to the logger it passes down:

| Layer | Fields |
| --- | --- |
| App | `app_name`, `app_id` (CLI: `app_name`) |
| Server | `server_name`, `server_id` |
| Worker internals | `broker`, `sub`, `sub_id` / `pub`, `pub_id` |
| Operation | `op_type`, `op_name`, `op_id` (+ `msg_id` for messages) |
| Recorder | `elapsed`, `err_code`, `err_severity`, `error`, plus transport fields |

### Log keys

Every key forgo logs under is a constant of the `logger` package, besides zerolog's own `level`, `time`, `message`, `error` and `caller`. The keys avoid `status` and `severity`, which log pipelines such as Datadog and Cloud Logging read as the level of a line.

| Key | Constant | Logged on | Value |
| --- | --- | --- | --- |
| `app_name` | `AppNameLogKey` | Every line of an App or a CLI | `WithName`, by default `default` (App) or `cli` (CLI) |
| `app_id` | `AppIDLogKey` | Every line of an App | An xid, new for each run |
| `server_name`, `server_id` | `ServerNameLogKey`, `ServerIDLogKey` | Every line of a server | `IServer.Name()`, and an xid for each server |
| `op_type` | `OperationTypeLogKey` | Every line of an operation | `http_handler`, `grpc_handler`, `cron_job`, `message_handler`, `command` |
| `op_name` | `OperationNameLogKey` | Every line of an operation | The route, gRPC method, job, message handler or command name |
| `op_id` | `OperationIDLogKey` | Every line of an operation | The request's `X-Request-ID` header or the call's `x-request-id` metadata when it is at most 128 bytes of visible ASCII, otherwise a new UUIDv7 |
| `elapsed` | `ElapsedTimeLogKey` | `operation completed`, `operation failed` | Seconds, as a float |
| `err_code` | `ErrorCodeLogKey` | `operation failed` | `operation.Error.Code` |
| `err_severity` | `ErrorSeverityLogKey` | Every error line | `0` light, `1` moderate, `2` critical |
| `cron_job`, `schedule` | `CronJobLogKey`, `CronScheduleLogKey` | `cron job added` | The job's name and schedule |
| `kind` | `KindLogKey` | HTTP and gRPC `operation started` and end lines, gRPC stream message lines | `request`, `response` |
| `addr`, `method`, `path`, `query` | `RequestAddrLogKey`, `RequestMethodLogKey`, `RequestPathLogKey`, `RequestQueryLogKey` | HTTP `operation started` | The client address (`RemoteAddr`), method, path and raw query, masked |
| `addr`, `path` | `RequestAddrLogKey`, `RequestPathLogKey` | gRPC `operation started` | The peer address and the full method, such as `/user.v1.UserService/GetUser` |
| `req_headers`, `req_body` | `RequestHeadersLogKey`, `RequestBodyLogKey` | HTTP and gRPC `operation started`, and `req_body` on gRPC `stream message received` | A JSON object of the headers or metadata; the body as JSON or a string, or the request message as JSON |
| `read_err` | `RequestBodyReadErrLogKey` | HTTP `operation started`, when reading the body failed | The error |
| `resp_status` | `ResponseStatusLogKey` | HTTP end lines | The status code, as an integer |
| `resp_headers`, `resp_body` | `ResponseHeadersLogKey`, `ResponseBodyLogKey` | HTTP and gRPC end lines, and `resp_body` on gRPC `stream message sent` | As for the request; on gRPC, the header metadata the call set, when it set any |
| `resp_trailers` | `ResponseTrailersLogKey` | gRPC end lines, when the call set trailer metadata | A JSON object of the trailer metadata, without the `x-request-id` added once the operation ended |
| `req_msgs`, `resp_msgs` | `RequestMessagesLogKey`, `ResponseMessagesLogKey` | gRPC end lines of client or bidi streams (`req_msgs`) and of server or bidi streams (`resp_msgs`) | The number of messages received or sent |
| `stream_seq` | `StreamSequenceLogKey` | gRPC `stream message received` and `stream message sent` | The message's number in its direction, from 1 |
| `grpc_code` | `GRPCCodeLogKey` | gRPC end lines | The name of the status code the client got, such as `OK` or `NotFound` |
| `headers_cut`, `body_cut` | `HeadersCutLogKey`, `BodyCutLogKey` | Next to logged headers, bodies and payloads | `true` when they were cut at the logging limit, or dropped |
| `msg_id` | `MessageIDLogKey` | Every line of a consumed message, and `publish failed` | `Message.ID` |
| `msg_headers`, `msg_payload` | `MessageHeadersLogKey`, `MessagePayloadLogKey` | Consumer `operation started` | A JSON object; the payload as JSON or a string |
| `msg_name` | `MessageNameLogKey` | Every line of a consumed message that no handler matches (operation `not_found`) | `Message.Name` |
| `msg_elapsed` | `MessageElapsedLogKey` | Consumer end lines, unless `Message.CreatedAt` is zero | Seconds since `Message.CreatedAt`, as a float |
| `msg_attempt` | `MessageAttemptLogKey` | Consumer end lines | The attempt counted by the inbox, 0 when processing failed before it was counted |
| `broker`, `sub`, `sub_id` | `BrokerLogKey`, `SubscriberLogKey`, `SubscriberIDLogKey` | Every line of a consumer's subscriber | The broker and subscriber names, and an xid |
| `pub`, `pub_id` | `PublisherLogKey`, `PublisherIDLogKey` | Every line of a producer's publisher, with `broker` | The publisher name, and an xid |
| `err_pct` | `ErrorPercentageLogKey` | `publish error percentage reached` | The failure rate of the batch, in percent |
| `live_config` | `LiveConfigLogKey` | Live config cache failures | The config's name |
| `sql_db` | `SQLDatabaseLogKey` | Every line of forgo-adapter's SQL query log | The `sqldb` resource's name |
| `sql_query`, `sql_query_cut` | `SQLQueryLogKey`, `SQLQueryCutLogKey` | Every line of the SQL query log | The statement, or `BEGIN`, `COMMIT` and `ROLLBACK`; `true` when it was cut at 32 KiB |
| `sql_args`, `sql_args_cut` | `SQLArgsLogKey`, `SQLArgsCutLogKey` | SQL query log lines, when arguments are logged | The arguments in placeholder order, masked; `true` when some were cut or left out |
| `sql_affected`, `sql_returned` | `SQLRowsAffectedLogKey`, `SQLRowsReturnedLogKey` | SQL query log lines of execs and queries | The rows affected, or the rows read |
| `sql_caller` | `SQLCallerLogKey` | Slow and failed statements of the SQL query log | The `file:line` that ran the statement |

### Masking sensitive fields

The HTTP server, the gRPC server and the worker consumer log what passes through them, so values such as passwords and tokens must not reach the logs as sent. Name them in the `SensitiveFields` of `http.Config`, `grpc.Config` or `worker.ConsumerConfig`, and every value logged under one of those names becomes `[*]`:

| Where | HTTP server | gRPC server | Worker consumer |
| --- | --- | --- | --- |
| Headers | Request and response headers. | Request and response metadata. | Message headers. |
| Query | Parameters, names unescaped first: `?token=abc&page=2` logs `token=[*]&page=2`. | | |
| Path | Wildcards of the route pattern: `/reset/{token}` logs `/reset/[*]`. A `{name...}` wildcard masks the rest of the path. | | |
| JSON body | Request and response bodies, at any depth. The whole value of a sensitive member is masked, objects and arrays included. | Request and response messages, by proto field name, the same way. | The payload, the same way. |
| Binding errors | The logged cause of a field that fails to bind. | | The same, for payload fields. |

Names match case-insensitively, the way `encoding/json` matches keys to struct fields: `password` masks `"Password"`, `"PASSWORD"` and the escaped `"pass\u0077ord"`. One list serves every place, so a name masks a header, a query parameter, a wildcard and a body member alike. The servers also always mask the credential headers `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie` and `X-Api-Key`, which match the lower-case gRPC metadata keys too.

Masking changes only what is logged. Handlers and clients still see every value as sent.

The rules live in the `redact` package, so code that logs input of its own masks it the same way: `redact.NewFields(names...)` builds the set once, `Fields.Has` matches a name, `Fields.LogJSON` adds a body to an event as the servers log it, and `Fields.AppendJSON` masks and compacts a JSON value into a buffer. Both treat JSON nested deeper than `redact.MaxJSONDepth` (1000) as invalid. forgo-adapter's SQL query log masks statement arguments with them.

**Bodies.** A body is logged as JSON only when it is complete and valid, and then compacted: the whitespace between its tokens is dropped, so an indented body does not split the log line. With `SensitiveFields`, a body cut at the logging limit, or one that is not JSON, is logged as a string up to the first byte that is not valid JSON, with `body_cut: true`. The rest is dropped, since it cannot be masked. So a form or `text/plain` body is logged as `""`, and `body_cut` then means the body was dropped, not that it was longer than the limit. A sensitive value that is cut short is still masked whole. Without `SensitiveFields`, a body that is not JSON or was cut is logged as sent, as a string.

**Binding errors.** The causes of binding errors can quote the rejected input, as in `strconv.ParseInt: parsing "1234": invalid syntax`, and the `Recorder` logs the cause. For a sensitive field it becomes `strconv.ParseInt: parsing "[*]": invalid syntax`, and the JSON number the decoder rejected is no longer quoted. The response (`errCode`, `errMsg`) does not change. A custom binder is covered as long as it returns bingo or `encoding/json` errors.

**Cost.** The masking rules are built once, before serving. With no `SensitiveFields`, the query and path are logged without being scanned, and a whole body is scanned once to check that it is JSON and compact it. With them, a body is scanned once to mask and compact it. Neither scan allocates, and neither runs for an event that is not logged, such as `operation started` above the info level.

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
| `logger.FromCtx(ctx) *zerolog.Logger` | The context's logger, or the default logger if none. It does not allocate: the logger is shared by every reader of the context, so derive a new one with `With()` instead of mutating it (`UpdateContext`, `Hook`, …). |
| `logger.NewCtx(ctx, zerolog.Logger) context.Context` | Store a logger in a context. |
| `logger.SetDefault(zerolog.Logger) (restore func())` | Make a logger the default. `restore` reinstates the previous default, unless a later `SetDefault` replaced it; call restores in reverse order. `App.Run` and `CLI.Run` call it for you. |
| `logger.FieldAdder` | `func(*operation.Context, zerolog.Context) zerolog.Context`, used by `middleware.Logger`. It runs once per operation and adds fields to every line of the operation. |
| `logger.EventFields` | `func(*operation.Context, *zerolog.Event)`, used by `middleware.Recorder`. It runs only for lines that are logged, so the operation must not rely on its side effects. |

Use the constants of [Log keys](#log-keys) when you query or extend the logs.

When logging errors yourself, follow the framework's convention and add the severity:

```go
logger.FromCtx(ctx).Error().Err(err).
	Int(logger.ErrorSeverityLogKey, int(operation.SeverityModerate)).
	Msg("payment provider unreachable")
```

---

## Metrics

Every server reports its operations to one contract, `operation.IObserver`, so one observer measures HTTP requests, gRPC calls, cron runs, messages and commands alike. Core depends on no metrics library: the observer records into the backend of your choice.

```go
type IObserver interface {
	Start(ctx *operation.Context)                       // before the operation runs
	End(ctx *operation.Context, o operation.Observation) // once it ended
}

type Observation struct {
	Elapsed time.Duration   // the time the operation took
	Err     error           // the error it failed with, nil on success
	Error   operation.Error // operation.ErrorOf(Err): its code, status and severity; zero on success
}
```

### Turning metrics on

Provide an `operation.IObserver` in the bootstrap. Every built-in server resolves it as an optional dependency, like `operation.ILocker`, and puts `middleware.Observe` in its base chain, directly inside `Recorder` (see [Order](#order)). There is nothing to configure per server.

forgo-adapter's OpenTelemetry module provides one, with a `MeterProvider` resource that exports over OTLP or to Prometheus (see its [REFERENCE.md](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#metrics-opentelemetry)):

```go
import (
	"github.com/efegreat/forgo-adapter/otel/meter"
	"github.com/efegreat/forgo-adapter/otel/observer"
)

app := forgo.NewApp(
	forgo.WithResources(meter.Config{ServiceName: "orders"}), // OTLP to the collector of OTEL_EXPORTER_OTLP_ENDPOINT
	forgo.WithProviders(observer.New(observer.Config{})),
	forgo.WithServers(config.HTTP, config.GRPC, config.Cron),
)
```

It records `forgo.operation.duration` with the labels below on every server, the HTTP semantic conventions' `http.server.request.duration`, and every [signal outside operations](#signals-outside-operations). Write an observer of your own for another backend (see [Writing an observer](#writing-an-observer)).

- Without an observer, the chains are the same as before and cost the same.
- Two providers whose results implement `IObserver` fail startup with `forgo.ErrAmbiguousDependency`.
- With `DisableBaseMiddlewares` there is no `Observe`. Add `middleware.Observe(o)` to your own chain, outside your `Recovery`.

### What an observer sees

`Observe` sits where the outcome of an operation is final:

- **Outside `ErrorResponder` and `Responder`.** When `End` runs, the HTTP response is written and the gRPC status is chosen, also for errors that a middleware returned and for calls grpc-go answered itself, such as a failed send.
- **Outside `Recovery`.** A panic reaches `End` as the `*middleware.PanicError` of `Recovery`, with the fallback error (code `internal`).
- **Outside `Tolerance`.** A cron run is one observation, its retries included.
- **Inside `Locker`.** A run skipped because another holds its lock is not observed, as it is not logged.
- **Inside `Recorder`.** Routes whose logging `ArgKeyRecorderDisabled` turns off are still observed, and `Recorder`'s own work, such as masking a body, is not measured.

| Server | `Type` | `Name` | Transport detail in `ctx.State` at `End` |
| --- | --- | --- | --- |
| HTTP | `http_handler` | route name; `not_found` | `Writer.StatusCode()` (0 with `Writer.Written()` for a hijacked connection), `Request.Method`, `Request.Pattern` (`"/"` for `not_found`) |
| gRPC | `grpc_handler` | full method; `unimplemented` | `Code()`, `FullMethod`. A stream is one observation. |
| Cron | `cron_job` | job name | none (`State` is nil) |
| Worker consumer | `message_handler` | handler name; `not_found` | `Attempts`, `Subscriber`. Redeliveries the idempotency guard settles from the inbox are observed too. |
| CLI | `command` | command name | `Fields` |

### Labels

Label by what means the same on every transport, so that one dashboard covers every server:

- `ctx.Type` and `ctx.Name`;
- the status: `ok` when `o.Err` is nil, and `o.Error.Status.String()` otherwise;
- the error code, `o.Error.Code`.

They are the labels of the answer the client got. The HTTP and gRPC servers return the error they answered, mapped through their `ErrorMapper` when it held no `operation.Error`, so `o.Error.Status` maps to the HTTP status written and to the gRPC code sent. The exceptions are a response a handler wrote itself through `State.Writer`, and a gRPC status that `MapStatus` sends as it is with a code no status maps to, such as `Unknown`. The transport detail in `State` always has the exact answer.

Labels must stay bounded. Never label by `ctx.ID`, a message ID, a raw path or anything the client chooses. On HTTP, label by `Request.Pattern`, never `Request.URL.Path`. A worker operation is named after its handler, and a message no handler matches runs as `not_found`, so producers cannot add names.

### Writing an observer

`Start` and `End` run on every operation's goroutine: they must be safe for concurrent use and must not block. `Start` may replace `ctx.Context`, as a middleware may.

An observer that records a duration histogram with OpenTelemetry:

```go
type Observer struct {
	duration metric.Float64Histogram
}

func NewObserver(mp metric.MeterProvider) (operation.IObserver, error) {
	h, err := mp.Meter("shop").Float64Histogram("operation.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	return &Observer{duration: h}, nil
}

func (o *Observer) Start(*operation.Context) {}

func (o *Observer) End(ctx *operation.Context, obs operation.Observation) {
	status := "ok"
	if obs.Err != nil {
		status = obs.Error.Status.String()
	}
	o.duration.Record(ctx, obs.Elapsed.Seconds(), metric.WithAttributes(
		attribute.String("op.type", ctx.Type),
		attribute.String("op.name", ctx.Name),
		attribute.String("status", status),
		attribute.String("error.code", obs.Error.Code),
	))
}
```

A histogram's count is the number of operations, so it needs no request counter beside it. Building the attribute set allocates on every operation; on hot paths, cache it per label set.

The `metric.MeterProvider` is resolved from the bootstrap like any dependency. Provide it from a resource that builds your SDK's provider and shuts it down in `Close`: the app closes resources after every server stopped, so the last metrics are flushed. Services that record business metrics take the same `metric.MeterProvider` in their constructors.

### Signals outside operations

Some outcomes are decided outside an operation, or are no operation at all. The observer is told about them when it also implements the interface of each. Servers check it once, in `New`, and skip the work otherwise.

| Interface | Told about | By |
| --- | --- | --- |
| `worker.IDeliveryObserver` | `Settled(ctx, worker.Settlement)`: how each delivery was settled at the broker (`Kind`: completed, retried or rejected), the `Attempt`, whether the idempotency guard settled a `Duplicate`, and the broker's `Err` | Worker consumer |
| `worker.IRelayObserver` | `Relayed(ctx, worker.RelayBatch)`: every batch of the outbox relay, the empty ones included, with the messages `Claimed` and `Published`, its `Elapsed` time and the `Err` of its claim or completion | Worker producer |
| `worker.IBacklogObserver` | `BacklogSampled(ctx, worker.OutboxBacklog)`: the messages of the outbox not published yet, `Pending`, up to `BacklogLimit`, and the creation time of the first of them, `Oldest`, every `BacklogInterval` | Worker producer, when its storage is a `worker.OutboxBacklogStorage` |
| `operation.ILockObserver` | `LockHeld(ctx, key)` and `LockFailed(ctx, key, err)`: the runs, messages and commands that `Locker` skipped or retried because another holder had their lock, or because it could not be acquired | Cron, worker consumer, CLI |
| `transport.IConnObserverFactory` | `ConnObserver(transport.Server{Name, Addr, ReportsActivity})`: each server asks for a connection observer of its own, so that its labels tell servers apart. What it returns may also be an `IConnIOObserver` or an `IConnStateObserver`. | HTTP, gRPC |

- **Settlements** are reported exactly once per delivery, also with `DisableBaseMiddlewares` and for the deliveries the consumer settles after the chain, from the goroutine that settled the delivery once the broker returned. A failed operation is not a lost message: it can be retried, and a redelivery can be completed from the inbox without running. Measure the rate of rejected messages here, not from `End`. `Message.CreatedAt` gives the time a message waited, and is zero when its producer did not send it.
- **Relay batches** report the relay's heartbeat: a relay that stopped reports nothing, while an idle one reports empty batches every `Interval`. `Claimed − Published` messages failed to publish and go back to the outbox.
- **The backlog** tells how far the relay is behind: the age of `Oldest` grows when the broker is down or the relay cannot keep up, while batches keep coming. The producer samples it only when both its storage (`worker.OutboxBacklogStorage`, which forgo-adapter's SQL outboxes implement) and the observer support it, on every instance: aggregate instances with `max`. `Pending` stops at `BacklogLimit`, since counting costs a read per pending row, while the age of `Oldest` keeps growing: alert on the age. A sample that fails is logged (`outbox backlog failed`) and skipped.
- **Lock keys** can hold message IDs: label locks by the operation that `operation.FromCtx(ctx)` finds, never by key. Servers wrap their `ILocker` with `operation.ObserveLocks`; custom servers can do the same.
- **Connection states** let an observer keep gauges of open connections. See [`transport`](#transport). `Config.Observer` of the HTTP and gRPC servers takes precedence over the bootstrap's.

---

## Background goroutines

Start background work from business logic with **`process.Go`** instead of a bare `go func()`, so the app waits for it before it exits.

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

**Why:** tracked goroutines are the only ones an App or CLI can wait for. On shutdown, the App terminates every server, then calls `process.WaitGoroutines()`, and only then closes resources. A bare `go func()` can be cut off mid-work when the process exits, or run against a database that has already been closed. `WaitGoroutines` also waits for goroutines started while it waits, such as those of a handler still running on a server that was killed.

Guidelines:

- The wait has no timeout. Background work must finish on its own, so give it a deadline (`context.WithTimeout(bg, …)`).
- Panics in the goroutine are not recovered. Recover inside it if the work can panic.
- Goroutines are tracked process-wide: two Apps in one process, such as parallel tests, wait for each other's.
- An HTTP request's context is cancelled when the request ends. Use `context.WithoutCancel(ctx)` for work that outlives the request.

Bounded concurrency:

| Type | Behavior |
| --- | --- |
| `process.NewRunner(sem)` | Never waits for a slot. `DoAsync(fn)` starts `fn` as a tracked goroutine if a slot is free and returns `process.ErrNoSlot` otherwise, without running `fn`. `TryAsync(fn)` runs `fn` in the caller when no slot is free, which slows a caller down to the limit instead of dropping work. |
| `process.NewConcurrentRunner(sem)` | `Run(ctx, fn)` waits for a slot and starts `fn`. When `ctx` ends first, it returns an error matching both `process.ErrNoSlot` and `ctx.Err()`, and does not run `fn`. `Wait()` waits for every started `fn`. Its goroutines are **not** tracked by `process.WaitGoroutines`. |
| `process.NewChannelSemaphore(n)` | `ISemaphore` with `n` slots (at least 1). `Acquire()` takes a slot without waiting or returns `process.ErrNoSlot`. `WaitAcquire(ctx)` waits for one, and fails once `ctx` ended even when a slot is free. `Release()` frees a slot and panics when none is taken. |

A runner frees its slot when `fn` returns. Runners built on one semaphore share its limit. `fn` must not be nil.

```go
var mailRunner = process.NewRunner(process.NewChannelSemaphore(20))

mailRunner.TryAsync(func() { _ = mailer.Send(bg, msg) })
```

---

## Live configs

A server config is read once, at startup. A **live config** can change while the application runs: feature flags, limits, prices or anything else an admin edits. It is a struct with a name and defaults:

```go
type Checkout struct {
	Enabled  bool   `json:"enabled"`
	MaxItems int    `json:"max_items"`
	Budget   uint64 `json:"budget"`
}

func (Checkout) Name() string      { return "checkout" }
func (Checkout) Default() Checkout { return Checkout{Enabled: true, MaxItems: 50} }
```

`Checkout` implements `forgo.LiveConfig[Checkout]`:

```go
type LiveConfig[T any] interface {
	Name() string // storage key: non-empty, unique, constant (it is called on the zero T)
	Default() T   // the values used for every field the stored document does not hold
}
```

Implement both methods on the value receiver. The configuration is stored as the JSON encoding of `T`, so give its fields json tags. `Load` decodes the stored document into the value `Default` returns, so `Default` must build its maps, slices and pointers on every call, like the literal above. A map or slice shared with a package variable would receive the stored values, and concurrent `Load`s would race on it.

`*forgo.LiveConfigs` loads and saves live configs. It reads a **cache** shared by every instance, such as Redis, and falls back to a **store**, the source of truth, such as a database table. Provide it after both:

```go
forgo.WithProviders(
	liveconfig.NewPostgres,   // forgo.LiveConfigStore, from forgo-adapter
	liveconfig.NewRedisCache, // forgo.LiveConfigCache, from forgo-adapter
	forgo.NewLiveConfigs,     // *forgo.LiveConfigs
	NewCartService,
)
```

Without a cache, provide `forgo.NewUncachedLiveConfigs` instead: every `Load` then reads the store.

```go
type CartService struct {
	configs *forgo.LiveConfigs
}

func NewCartService(configs *forgo.LiveConfigs) *CartService {
	return &CartService{configs: configs}
}

type AddItemReq struct {
	CartID string `uri:"cart_id" validate:"required"`
	ItemID string `json:"item_id" validate:"required"`
}

type AddItemResp struct {
	Items int `json:"items"`
}

func (s *CartService) AddItem(ctx context.Context, req AddItemReq) (AddItemResp, error) {
	cfg, err := s.configs.Load[Checkout](ctx)
	if err != nil {
		return AddItemResp{}, err // the store failed: do not continue
	}
	if !cfg.Enabled {
		return AddItemResp{}, ErrCheckoutClosed
	}
	// … add req.ItemID to the cart, up to cfg.MaxItems items
	return AddItemResp{}, nil
}
```

### Loading

`Load[T](ctx)` takes these steps, and never waits for another caller:

1. Start from `Default()`.
2. If the config is [kept in memory](#keeping-a-config-in-memory) and its document is younger than its `MemoryTTL`, decode it over the defaults and return it.
3. Read the cache. If it holds a document, decode it over the defaults and return it.
4. Otherwise read the store. If it holds a document, decode it over the defaults and return it.
5. Otherwise return `Default()`.

Decoding over the defaults means:

- Fields in the document replace the defaults, zero values included, so an admin can save `false` or `0`.
- Fields the document lacks keep their default. A field you add to `T` later works at once, without migrating stored documents.
- Values decode straight into the typed fields, so a `uint64` or `int64` keeps its full precision. Fields of type `any` follow `encoding/json` and hold numbers as `float64`.
- Maps are merged: stored keys are added to the map `Default` returns, so an admin cannot remove a key that `Default` sets. Leave such maps empty in `Default`.

Only the store's failures are critical:

| Failure | `Load` |
| --- | --- |
| The cache fails, or its document does not decode into `T` | Logs it at error level with `err_severity` moderate (1) and the config's name under `live_config`, and reads the store. |
| The store fails, or its document does not decode into `T` | Returns the error, with `Default()`. Do not continue on it. |

`Load` never writes the shared cache: only `Save` does. A config that is stored but not cached, for example after the cache was flushed, is read from the store on every `Load` until it is saved again. Every call returns a new value, so callers may modify it freely, as long as `Default` builds a new value too.

### Saving

```go
type UpdateCheckoutReq struct {
	Checkout Checkout `json:"checkout"`
	Version  uint64   `json:"version"` // the version the edit started from, 0 when nothing is stored
}

type UpdateCheckoutResp struct {
	Version uint64 `json:"version"`
}

func (s *CartService) UpdateCheckout(ctx context.Context, req UpdateCheckoutReq) (UpdateCheckoutResp, error) {
	if err := s.configs.Save(ctx, req.Checkout, req.Version); err != nil { // T is inferred
		return UpdateCheckoutResp{}, err
	}
	return UpdateCheckoutResp{Version: req.Version + 1}, nil
}
```

Every stored document has a version, which starts at 1 and grows by one with each save. `Save(ctx, cfg, version)` takes the version the change started from, 0 when nothing was stored:

1. It stores `cfg` as `version+1`, only if the stored version is still `version`. When another save came first, it returns `forgo.ErrLiveConfigConflict` and changes nothing, so of two admins editing the same version, one succeeds and the other is told to reload.
2. Once the store succeeded, it caches the document at `version+1`, even if `ctx` was canceled in between. The cache keeps the newest version, so saves that reach it out of order never cache an older version over a newer one.

`Save` replaces the whole document. Its errors:

| Error | Meaning |
| --- | --- |
| `forgo.ErrLiveConfigConflict` | The stored version is not `version`. Nothing changed. |
| `forgo.ErrLiveConfigNotCached` | The document was stored, but the cache could not be updated. The cache entries never expire, so instances may serve the previous version until the config is saved again. |
| any other | The encoding or the store failed. Nothing changed. |

Once the cache holds the saved version, every instance serves it on its next `Load`, or within its `MemoryTTL` for a config kept in memory.

### Keeping a config in memory

By default, every `Load` reads the cache, so a save reaches every instance at once. A config that can take a short delay saves that round trip by implementing `forgo.LiveConfigMemoryTTL`:

```go
func (Checkout) MemoryTTL() time.Duration { return 30 * time.Second }
```

Each instance then keeps what `Load` read from the cache or the store, including the absence of a document, for `MemoryTTL`:

- Within the TTL, `Load` reads nothing. It decodes the kept document into a new value, so callers still never share one.
- Only documents read successfully are kept. A `Load` that fails keeps nothing.
- After a `Save`, the instance that saved serves the new version at once, and the other instances within `MemoryTTL`. When `Save` returns `ErrLiveConfigNotCached`, the instance that saved serves what the cache serves, like the others.
- A `Load` that read the cache before a `Save` on the same instance never keeps the older document over the saved one.
- Once the TTL has passed, each `Load` that finds it so reads the cache itself. None waits for another.

Leave `MemoryTTL` out of configs that must change everywhere at once, such as a kill switch. Zero or less keeps nothing in memory. Declare it on the value receiver, like `Name` and `Default`.

### Admin endpoints

Admins read and change live configs through two HTTP handlers. List the configs they may see in a catalog, declared once:

```go
// admin/configs.go
var Configs = forgo.LiveConfigCatalog{
	forgo.LiveConfigOf[checkout.Settings](),
	forgo.LiveConfigOf[pricing.Settings](),
}
```

Then route both handlers:

```go
Routes: []http.RouteConfig{
	{Name: "live_configs", Method: "GET", Path: "/admin/configs",
		Handler: http.LiveConfigsHandler(admin.Configs), Middlewares: adminOnly, DisableAPIDoc: true},
	{Name: "set_live_config", Method: "PUT", Path: "/admin/configs/{name}",
		Handler: http.SetLiveConfigHandler(admin.Configs), Middlewares: adminOnly, DisableAPIDoc: true},
},
```

They are ordinary routes. Anyone who reaches them reads or changes every config of the catalog, so protect them with middlewares, such as `JWTAuth` and a role check. `DisableAPIDoc` keeps them out of the API documents. The server's `New` fails when the catalog lists a name twice, or when no `*forgo.LiveConfigs` is provided. The catalog only scopes these endpoints: `Load` and `Save` work with every config.

**Reading.** `http.LiveConfigsHandler` answers with every config of the catalog, keyed by name, as the result of the serializer's envelope:

```json
{
  "checkout": {"version": 3, "value": {"enabled": true, "max_items": 20}},
  "pricing":  {"version": 0, "value": {"rate": 1.5}, "memory_ttl": "30s"}
}
```

- It reads the store, never the cache or memory, so admins see the state that saves are checked against.
- `value` is the config that instances serve: the defaults completed with the stored document. Version 0 means nothing is stored.
- `memory_ttl` is set for configs [kept in memory](#keeping-a-config-in-memory): instances may serve the previous value that long after a change.

**Changing.** `http.SetLiveConfigHandler` changes the config named by the `{name}` path wildcard. The body is the whole config, as the `value` the read answers, and `If-Match` holds the version the change started from:

```http
PUT /admin/configs/checkout
If-Match: 3

{"enabled": true, "max_items": 25}
```

1. The body completes the config's defaults, so a field it leaves out gets its default, not its current value. A field the config does not have is rejected, so a misspelled field is never saved without effect.
2. The server's `Binder` validates the config like a request body, with its `validate` tags and `Config.Validators`.
3. A config equal to the stored one stores nothing, whatever `If-Match` holds. The cache is then given the stored version, which completes an earlier change that answered `live_config_not_cached`.
4. Otherwise the config is saved only while the stored version is still the one in `If-Match`.

The response holds the stored version and whether the request changed it: `{"version": 4, "changed": true}`.

| Status | `errCode` | When |
| --- | --- | --- |
| 404 | `live_config_not_found` | The catalog has no config of that name. |
| 400 | `live_config_version` | `If-Match` is missing or is not a version, such as `3` or `"3"`. |
| 400 | `live_config_invalid` | The body is not a JSON object of the config's fields, or is larger than 1 MiB. |
| 400 | `validation` | A `validate` rule failed. |
| 412 | `live_config_conflict` | The config was changed since the version in `If-Match`. Read it again. |
| 503 | `live_config_not_cached` | The change is stored, but the cache missed it. Send the same request again. |

Request logs record the body, like any request: keep secrets out of live configs, or list their fields in the server's `SensitiveFields`.

The same operations are available in Go. `LiveConfigs.LoadStored[T](ctx)` returns the stored config with its version, and `LiveConfigs.SaveChanged(ctx, cfg, version)` saves it only when it changed, returning the stored version and whether it saved.

### Changing a live config type

During a rolling deployment, old and new versions of the application run side by side and share the stored documents:

- **Add fields freely.** Old instances ignore the fields they do not know. Save new values only once the deployment has completed: an instance running the old code writes the document without the new fields, and their saved values are lost.
- **Never change a field's type.** Instances that cannot decode the document fail every `Load` of it. Add a field with a new name instead.

### Stores and caches

```go
type LiveConfigStore interface {
	// Get returns the document stored under name and its version. Version 0 means nothing is stored.
	Get(ctx context.Context, name string) (data []byte, version uint64, err error)
	// Set stores data under name as version+1, only while the stored version is version
	// (0: nothing is stored yet). Otherwise it returns forgo.ErrLiveConfigConflict and changes nothing.
	Set(ctx context.Context, name string, data []byte, version uint64) error
}

type LiveConfigCache interface {
	// Get returns the document cached under name, and false when there is none.
	Get(ctx context.Context, name string) (data []byte, found bool, err error)
	// Set caches data under name at version, unless the entry holds the same or a newer version.
	Set(ctx context.Context, name string, data []byte, version uint64) error
}
```

Both only move JSON documents: `LiveConfigs` encodes and decodes them. A missing document is not an error. The caller keeps the data `Get` returns, so it must not be modified afterwards. A store's `Set` must be committed when it returns, and a cache's entries must never expire.

forgo-adapter provides the SQL stores `liveconfig.NewPostgres` and `liveconfig.NewMySQL`, and the Redis cache `liveconfig.NewRedisCache` (see its [REFERENCE.md](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#liveconfig)). The SQL stores use this table:

```sql
-- PostgreSQL
CREATE TABLE live_configs (
    name       TEXT        PRIMARY KEY,
    data       JSONB       NOT NULL,
    version    BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- MySQL 8
CREATE TABLE live_configs (
    name       VARCHAR(255)    NOT NULL PRIMARY KEY,
    data       JSON            NOT NULL,
    version    BIGINT UNSIGNED NOT NULL,
    updated_at DATETIME(6)     NOT NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;
```

Reads and saves go by name, through the primary key. On MySQL, `utf8mb4_0900_bin` keeps names case-sensitive, as `LiveConfig.Name` is.

---

## Utilities

### `types.Map` and `types.JSONMap`

```go
m := types.NewMap[string, int]()                // or types.NewMapWithData(map[string]int{...})
m.Set("a", 1)
v, ok := m.Get("a")
n := m.Len()
m.Delete("a")
snapshot := m.Clone()                           // shallow copy of the underlying map
for k, v := range m.Range {                     // or m.Range(func(k string, v int) bool { ... })
	fmt.Println(k, v)
}

i := m.GetTyped[int]("a")                       // typed read; zero value if absent or mismatched
```

`GetTyped[T]` returns a value that is a `T` as is (for an interface `T`, one that implements it). It also converts `json.Number` values to `int`, `int8` … `int64`, `uint` … `uint64`, `float32` and `float64` without losing precision. Only those exact types convert, not a named type such as `type Port int`, and integers convert only when written without a fraction or exponent and within range. Anything else gives the zero value.

`Range` holds the read lock for the whole iteration, so it sees a consistent view without copying the map. Inside the loop, don't call methods of the same map: a write deadlocks, and so can a read while a writer waits. Use `Clone` when the loop body needs the map.

The zero `Map` is empty and ready to use, and lookups in an empty map and `Len` skip the lock. A nil `*Map` reads like a nil Go map: `Get`, `GetTyped`, `Len`, `Range` and `Clone` find nothing, `Delete` does nothing and `Set` panics. `Clone` returns nil when the `Map` has no map yet, such as the zero `Map`, so check it before writing to it. `types.NewMapCopyOnWrite(data)` reads `data` in place and clones it before its first write. Use it for static data shared by many maps, such as route args, and never modify `data` afterwards. `m.ResetCopyOnWrite(data)` does the same for a `Map` held by value, and drops what `m` held: the servers keep the `Args` of each operation in one struct with the operation and its `State`, allocated once.

`types.JSONMap[V]` is a `Map[string, V]` that encodes as a JSON object. It implements `json.Marshaler` and `json.Unmarshaler`, so it keeps its entries in responses and request bodies, and `driver.Valuer` and `sql.Scanner`, so it can be stored in a JSON column as is. Message headers use it.

- `Value` returns the object as a `string`, which every driver sends as JSON text. MySQL rejects JSON built from a `[]byte` once the driver interpolates parameters (`interpolateParams=true`).
- `Scan` and `UnmarshalJSON` replace the entries, reject anything but one JSON object (or `null`) and leave the map unchanged on error. They decode numbers in interface values, such as the values of a `JSONMap[any]` and anything nested in them, as `json.Number`, so `uint64` IDs keep their precision.
- A `JSONMap` without a map encodes as `null` and stores as SQL `NULL`. That is a nil `*JSONMap`, the zero `JSONMap`, and one that scanned `NULL` or decoded `null`. `NewJSONMap`, `NewJSONMapWithData` and the first write give it a map, so it encodes as an object, `{}` when empty.
- Hold it by pointer, as `*types.JSONMap[V]`: `encoding/json` calls its methods only on addressable values. The methods it promotes from `Map` (`Get`, `Range` and so on) panic on a nil `*JSONMap`.

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
| `transport.IConnObserver` | `OnAcceptError`, called for every failed `Accept` except after the listener is closed. |
| `transport.IConnIOObserver` | An `IConnObserver` with `OnReadError` and `OnWriteError` too. |
| `transport.IConnStateObserver` | An `IConnObserver` with `OnConnState(conn, state)` too, for gauges of open connections. |
| `transport.ConnState` | `ConnNew`, `ConnActive`, `ConnIdle`, `ConnHijacked`, `ConnClosed`, named by `String()` (`"idle"`). |
| `transport.IConnObserverFactory` | `ConnObserver(transport.Server) IConnObserver`, for an `operation.IObserver` of the bootstrap that observes the connections of each server. |
| `transport.Server` | The server an observer is asked for: its `Name` (`ServerConfig.Name`, or its default name), its `Addr`, and `ReportsActivity`, set by servers that report `ConnActive`, `ConnIdle` and `ConnHijacked`. |
| `transport.Listener` | A `net.Listener` that reports to its `Observer`, and reports nothing when `Observer` is nil. With `ReportState`, it also reports `ConnNew` and `ConnClosed` to an `IConnStateObserver`. |

Observers are for metrics and alerts. Accept errors are already logged: net/http logs the ones it retries through `ErrorLog`, and `App` logs the one `Serve` returns. Hooks run concurrently, on the accept loop and on the connections' goroutines, so they must not block.

**Connection states.** The HTTP server reports every state through net/http's `ConnState` hook, which wraps no connection, then calls the `ConnState` of its config. net/http forgets a hijacked connection, so the server reports it closed once its handler returns. The gRPC server reports only `ConnNew` and `ConnClosed`, since HTTP/2 serves many calls on one connection, through a `Listener` with `ReportState`. `transport.Server.ReportsActivity` tells an `IConnObserverFactory` which of the two a server does, so that it counts the connections of a server without it as open from `ConnNew` to `ConnClosed`, whatever the server is named. Every connection ends with one `ConnClosed`, and `conn` is the same value for every state of one connection, so an observer can keep the last state of each.

The listener wraps accepted connections only for an `IConnIOObserver`, or for an `IConnStateObserver` with `ReportState`. A wrapper hides the zero-copy paths of `*net.TCPConn`, so the other observers do not pay for it. A wrapped connection forwards `CloseWrite`. It also has `NetConn()`, which returns the accepted connection, like `*tls.Conn` does. `ConnState` and `ConnContext` hooks can use it to reach the `*net.TCPConn`. The read and write hooks receive the accepted connection and report failures only:

- They are not called for the end of the stream (`io.EOF`), for a connection closed on this side (`net.ErrClosed`), or for I/O cut short by a deadline that had already passed when it was set. net/http cancels its own background read that way after every request.
- Each direction reports its first failure only, until its deadline changes. net/http retries a read that timed out, and the retries fail the same way.
- Expired deadlines are reported. net/http uses read deadlines for `ReadTimeout`, `ReadHeaderTimeout` and `IdleTimeout`, so an idle keep-alive connection that times out reports an error matching `os.ErrDeadlineExceeded`, like a client too slow to send its request.

```go
// connErrors counts connection failures for a metrics endpoint.
type connErrors struct{ accept, read, write atomic.Int64 }

func (c *connErrors) OnAcceptError(error)          { c.accept.Add(1) }
func (c *connErrors) OnReadError(net.Conn, error)  { c.read.Add(1) }
func (c *connErrors) OnWriteError(net.Conn, error) { c.write.Add(1) }

var server = http.Config{Observer: &connErrors{}, Routes: routes}
```

---

## Adapters

Concrete infrastructure lives in a separate library, [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/README.md), so forgo itself never depends on a database driver, broker client or cache client. Its [REFERENCE.md](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md) documents every config field and default.

| Adapter | Import path | Kind | Provides | Notes |
| --- | --- | --- | --- | --- |
| `sqldb` | `github.com/efegreat/forgo-adapter/resource/sqldb` | resource | `*sql.DB` | Applies pool limits and pings on startup. Import your SQL driver in `main`. An optional query log (`Logger`) writes each statement through the context's logger, at warn when it is slower than `SlowThreshold` and at error when it fails, optionally with its arguments masked by `SensitiveFields`. |
| `redisdb` | `github.com/efegreat/forgo-adapter/resource/redisdb` | resource | `redis.UniversalClient` | One server, a Redis Cluster or a Sentinel-managed master, chosen from `Addrs`, `MasterName` and `ClusterMode` like `redis.NewUniversalClient`. `ReadFrom` sends read-only commands to replicas. Pings on startup. The Redis providers below request `redis.UniversalClient`. |
| `rabbitmq` | `github.com/efegreat/forgo-adapter/resource/broker/rabbitmq` | resource | `worker.IBroker` | Connects with TLS when `TLSConfig` is set. Declares exchanges, queues and bindings at startup. Named publishers and subscribers match worker config names. Reconnects automatically, waits for publisher confirms, and sets the consumer prefetch to the subscriber's `Concurrency` by default. Optional per-queue dead-letter queues (`DeadLetter`) keep the messages the consumer rejects, and retry queues (`Retry`) delay redeliveries exponentially. |
| `pubsub` | `github.com/efegreat/forgo-adapter/resource/broker/pubsub` | resource | `worker.IBroker` | Google Cloud Pub/Sub. Creates the listed topics and subscriptions at startup and checks the settings of existing subscriptions. Messages go to the topic named by `Destination`, with `Key` as their ordering key, and publishers share one batching client publisher per topic. The number of messages a subscriber holds defaults to the subscriber's `Concurrency`. Optional subscription retry policies (`Retry`) delay redeliveries, and dead-letter topics (`DeadLetter`) keep the messages the consumer rejects. |
| `redisstream` | `github.com/efegreat/forgo-adapter/resource/broker/redisstream` | resource | `worker.IBroker` | Redis Streams, over a client of its own built from a `redisdb.Config`. Creates the listed consumer groups at startup. Messages go to the stream named by `Destination`, optionally trimmed (`MaxLen`). Each subscriber reads its group as a consumer of its own, no more entries than it has free slots, keeps the entries it handles from being claimed, and claims the ones a stopped consumer left pending for `ClaimIdle`. Optional delayed retries (`Retry`) and dead-letter streams (`DeadLetter`). |
| `sqltx` | `github.com/efegreat/forgo-adapter/provider/sqltx` | provider | `operation.Transactor` | Carries a `database/sql` transaction in the context. `sqltx.From(ctx, db)` joins it from any repository or storage. |
| `inbox` (Postgres, MySQL) | `github.com/efegreat/forgo-adapter/provider/inbox` | provider | `worker.InboxStorage` | Uses the `inbox_events` table (see [Storage contracts](#storage-contracts)). `MarkCompleted` succeeds only for the delivery that completes the message, so a concurrent duplicate rolls back its handler's transaction. |
| `outbox` (Postgres, MySQL) | `github.com/efegreat/forgo-adapter/provider/outbox` | provider | `worker.OutboxStorage`, `worker.OutboxBacklogStorage` | Uses the `outbox_events` table (see [Storage contracts](#storage-contracts)). `Save` writes a message in the transaction of its context. Claims with `FOR UPDATE SKIP LOCKED`. Reports its backlog for metrics, through the index of the pending rows. |
| `locker` (Redis) | `github.com/efegreat/forgo-adapter/provider/locker` | provider | `operation.ILocker` | `SET NX` with TTL. Unlock only releases a lock this process still holds. |
| `ratelimit` (Redis) | `github.com/efegreat/forgo-adapter/provider/ratelimit` | provider | `ratelimit.ILimiter` | GCRA in one Lua script on the Redis clock, for the HTTP and gRPC servers' `RateLimitWith` across instances. |
| `liveconfig` (Postgres, MySQL, Redis) | `github.com/efegreat/forgo-adapter/provider/liveconfig` | provider | `forgo.LiveConfigStore`, `forgo.LiveConfigCache` | SQL stores on the `live_configs` table (see [Live configs](#live-configs)) with versioned conditional saves, outside any transaction the context carries. A Redis cache whose entries never expire and keep the newest version. |
| `meter` | `github.com/efegreat/forgo-adapter/otel/meter` | resource | `metric.MeterProvider` | OpenTelemetry's SDK, exporting over OTLP (HTTP or gRPC) or to Prometheus through `meter.PrometheusRoute()`, with Go runtime metrics. `Close` exports what is left. In the module `github.com/efegreat/forgo-adapter/otel`, so that the other adapters never pull in OpenTelemetry. |
| `observer` | `github.com/efegreat/forgo-adapter/otel/observer` | provider | `operation.IObserver` | `observer.New(observer.Config{})` records the [metrics](#metrics) of every server with the bootstrap's `metric.MeterProvider`, the signals outside operations included, without allocating per operation. In the module `github.com/efegreat/forgo-adapter/otel`. |

```go
forgo.NewApp(
	forgo.WithResources(
		sqldb.Config{Name: "main", DriverName: "postgres", DSN: dsn},
		redisdb.Config{Name: "cache", Addrs: redisAddrs},
		brokerConf, // rabbitmq.Config, pubsub.Config or redisstream.Config
	),
	forgo.WithProviders(sqltx.NewTransactor, inbox.NewPostgres, outbox.NewPostgres, locker.NewRedis, ratelimit.NewRedis),
	forgo.WithServers(httpConf, consumerConf, producerConf),
).Run()
```

Adapters are ordinary resources and providers. Anything you write yourself plugs in the same way: implement `forgo.ResourceFactory`, or write a constructor and list it in `WithProviders`.

---

## Error code reference

| Code | Origin | Status | Meaning |
| --- | --- | --- | --- |
| `internal` | any | Unknown → HTTP 500, gRPC `Internal` | Unmapped error (`operation.NewFallbackError`), panics included |
| `route_not_found` | HTTP | 404 | No route matches the request |
| `terminating` | HTTP `HealthHandler` | 503 | The server's `TermSig` is signaled |
| `live_config_not_found` | HTTP `SetLiveConfigHandler` | 404 | The catalog has no config of that name |
| `live_config_version` | HTTP `SetLiveConfigHandler` | 400 | `If-Match` is missing or is not a version |
| `live_config_invalid` | HTTP `SetLiveConfigHandler` | 400 | The body is not a JSON object of the config's fields, or is larger than 1 MiB |
| `live_config_conflict` | HTTP `SetLiveConfigHandler` | 412 | The config was changed since the version in `If-Match` |
| `live_config_not_cached` | HTTP `SetLiveConfigHandler` | 503 | The change is stored, but the cache missed it; send it again |
| `body_too_large` | HTTP | 413 | Request body is larger than `MaxBodyBytes` |
| `eof` | HTTP | 400 | Request body is truncated |
| `json_syntax` | HTTP | 400 | Malformed JSON body |
| `json_type` | HTTP | 400 | JSON value has the wrong type |
| `invalid_body` | HTTP | 400 | Any other body the binder rejects, such as two JSON documents or a field bound from another source |
| `binding` | HTTP | 400 | Query, path or header value could not be bound |
| `validation` | HTTP | 400 | `validate` rule failed |
| `unauthenticated` | `JWTAuth` (`jwtauth.ErrUnauthenticated`) | 401, `Unauthenticated` | The request has no bearer token |
| `invalid_token` | `JWTAuth` (`jwtauth.ErrInvalidToken`) | 401, `Unauthenticated` | The token cannot be verified or its claims are not accepted |
| `token_expired` | `JWTAuth` (`jwtauth.ErrTokenExpired`) | 401, `Unauthenticated` | The token is valid but expired |
| `auth_unavailable` | `JWTAuth` (`jwtauth.ErrAuthUnavailable`) | 503, `Unavailable` | The keys to verify the token cannot be fetched |
| `rate_limited` | `RateLimit` (`ratelimit.ErrLimited`) | 429, `ResourceExhausted` | The client exceeded the rate |
| `rate_limit_unavailable` | `RateLimit` (`ratelimit.ErrUnavailable`) | 503, `Unavailable` | The rate limiter failed and `FailOpen` is off |
| `rejected` | `middleware.Guard` | PermissionDenied → HTTP 403 | A `Guard` without a `reject` function denied the operation (`middleware.ErrRejected`) |
| `unimplemented` | gRPC | `Unimplemented` | No configured method matches the call |
| `binding` | gRPC | `InvalidArgument` | A request value its handler's struct cannot hold, such as a number out of range |
| `validation` | gRPC | `InvalidArgument` | `Config.Validator` rejected a request |
| `nil_response` | gRPC | `Internal` | A unary or client-streaming handler returned a nil response without an error, or a middleware answered without one |
| `invalid_response` | gRPC | `Internal` | A response its message cannot hold, such as an unknown enum name or two members of a `oneof` |
| `canceled` | gRPC | `Canceled` | The handler returned `context.Canceled`, or a receive or send was canceled |
| `deadline_exceeded` | gRPC | `DeadlineExceeded` | The handler returned `context.DeadlineExceeded`, or the deadline passed during a receive or send |
| `message_too_large` | gRPC | `ResourceExhausted` (sent by grpc-go) | A request message is over `MaxRecvMsgSize` |
| `invalid_message` | gRPC | `InvalidArgument`, or `Internal` (sent by grpc-go) | A request message cannot be decoded (`InvalidArgument`), or a unary or server-streaming method got zero or several (`Internal`) |
| `send_failed` | gRPC | grpc-go's, such as `ResourceExhausted` | A response message cannot be sent, such as one over `MaxSendMsgSize` |
| `unknown_service` | gRPC `HealthService` | `NotFound` | `Check` asked about a service the server does not serve |
| `not_found`, `already_exists`, … | gRPC `MapStatus` | the status's own | A handler returned a gRPC status without an `ErrorInfo`; its code in snake case |
| `msg_bind` | worker | InvalidArgument | Payload could not be decoded or validated; message rejected |
| `msg_ack` | worker | Internal | Acknowledging the delivery failed |
| `msg_locked` | worker | Aborted | Another delivery holds the handler's `Lock`; message retried |
| `msg_timeout` | worker | Timeout | The handler returned an error after its `HandlerConfig.Timeout` |
| `msg_attempts` | worker | Aborted | The message came back after its last attempt ended without a verdict; marked failed and rejected |
| `msg_no_handler` | worker | InvalidArgument | No handler for the message name; message rejected |
| `command_parse` | CLI | InvalidArgument | Command flags could not be parsed |
| `command_validate` | CLI | InvalidArgument | `Command.Validate` failed |
| `command_locked` | CLI | Aborted | Another run holds the command's `Lock`; the command did not run |

Bootstrap errors are returned before anything serves, by `Bootstrap.Build`, `App.Run` and `cli.CLI.Run`. They start with `bootstrap:` and name the provider, argument and type involved. Check them with `errors.Is` against `forgo.ErrMissingDependency`, `forgo.ErrAmbiguousDependency` and `forgo.ErrResourceNotFound`.
