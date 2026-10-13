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
| **Server** | A long-running unit that serves work: HTTP, cron, worker, or your own. You declare it as a config value that implements `ServerFactory`. | `github.com/efegreat/forgo/server/...` |
| **App** | Runs one or more servers as a whole and manages their lifecycle: start, graceful termination, forced termination. | `github.com/efegreat/forgo` |
| **CLI** | Runs one named command per invocation, using the same resource/provider model as an App. | `github.com/efegreat/forgo/cli` |
| **Operation** | The transport-agnostic unit of work. An HTTP request, a cron run, a consumed message and a CLI command are all operations. | `github.com/efegreat/forgo/operation` |
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
 │    TermSig.Signal() + wait → Terminate(ctx with TermTimeout) → Kill() on timeout
 ├─ process.WaitGoroutines()      wait for tracked background goroutines
 └─ Bootstrap.Close()             resources closed in reverse creation order
```

No reflection or dependency lookup happens after `Seal`. Request handling runs pre-built handler chains only.

### Packages

| Import path | Contents |
| --- | --- |
| `github.com/efegreat/forgo` | `App`, `Bootstrap`, `Resolver`, `Resource`, `ServerFactory`, `IServer`, `ServerConfig`, `MiddlewareFactory`, `LiveConfig`, `LiveConfigMemoryTTL`, `LiveConfigs`, `LiveConfigStore`, `LiveConfigCache`, `LiveConfigCatalog`, `Env` |
| `github.com/efegreat/forgo/operation` | `Context`, `FromCtx`, `Handler`, `Middleware`, `Chain`, `Error`, `Status`, `ErrorMapper`, `Lock`, `ILocker`, `Tolerance`, `Transactor` |
| `github.com/efegreat/forgo/middleware` | Built-in middlewares: `ID`, `Logger`, `Recorder`, `Recovery`, `Locker`, `Tolerance`, `Guard`, plus `PanicError`, `ErrRejected` and `ErrNoLocker` |
| `github.com/efegreat/forgo/server/http` | HTTP server, plus opt-in `CORS`, `RateLimit` and `JWTAuth` middlewares and live config admin handlers |
| `github.com/efegreat/forgo/server/cron` | Cron server |
| `github.com/efegreat/forgo/server/worker` | Message consumer and outbox producer servers, broker and storage contracts |
| `github.com/efegreat/forgo/cli` | Command-line runner |
| `github.com/efegreat/forgo/commands/migration` | SQL migration command |
| `github.com/efegreat/forgo/commands/openapi` | OpenAPI 3.1 generator command |
| `github.com/efegreat/forgo/logger` | Logger configuration, context propagation, log keys |
| `github.com/efegreat/forgo/process` | Tracked goroutines, runners, semaphores |
| `github.com/efegreat/forgo/transport` | Serializer and connection observer contracts |
| `github.com/efegreat/forgo/types` | Concurrency-safe `Map` and SQL-ready `JSONMap` |

### Requirements

- **Go 1.27 or newer.** Forgo uses generic methods, such as `r.Resolve[T]()` and `m.GetTyped[T](key)`.
- The package `github.com/efegreat/forgo/server/http` is named `http`. When a file also needs the standard library, alias one of them, for example `nethttp "net/http"`.

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

	"github.com/efegreat/forgo"
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

1. Creates the app logger with `app_name` and `app_id` fields, and makes it the default logger (`logger.SetDefault`) until `Run` returns, so resources, providers and code that log without an operation's context use it too.
2. Builds the bootstrap: resources, then providers.
3. For each server factory, in order: calls `New(resolver)`, derives a server logger with `server_name` and `server_id`, and calls `Init(ctx)` with that logger in `ctx`.
4. Seals the bootstrap.
5. Starts every server's `Serve()` in its own goroutine.
6. Waits for SIGINT, SIGTERM, or the first `Serve` error.
7. Terminates all servers **in parallel**. For each one:
   1. If `Term().TermSig` is set, it is signaled and the app sleeps for `TermSig.Wait`. This is the window for a readiness probe to fail and load balancers to drain.
   2. Calls `Terminate(ctx)`. `ctx` carries the server's logger and expires after `Term().TermTimeout`, which defaults to 30s.
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

func FromCtx(ctx context.Context) (*Context, bool)
```

- It is a `context.Context`. Handlers and services receive it as their `ctx`.
- `Value(key)` checks `Args` first, then the embedded context. Static values configured on a route or message handler (`Args: map[any]any{...}`) are in `Args` for each operation, so `ctx.Value(myKey)` returns them anywhere downstream. The server copies that map once at startup and every operation reads the copy in place. An operation that writes to its `Args` gets a private copy first, so writes never reach other operations. Changing the config map after startup has no effect.
- Use your own unexported key types for `Args`, as for `context.WithValue`. The framework's keys, such as `http.ArgKeyRecorderDisabled`, each have a type of their own package.
- Middlewares replace `Context` before they call the next handler, and `Tolerance` resets it before every attempt. A goroutine the operation starts may read the fields, but write only to `Args`.

| Transport | `Type` | `State` |
| --- | --- | --- |
| HTTP | `http.OperationTypeHTTPHandler` (`"http_handler"`) | `*http.State{Request *http.Request, Writer}` |
| Cron | `cron.OperationTypeCronJob` (`"cron_job"`) | `nil` |
| Worker consumer | `worker.OperationTypeMessageHandler` (`"message_handler"`) | `*worker.State{Delivery worker.IDelivery, Attempts int}` |
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

`Map` returns the first `operation.Error` in the error's chain unchanged, without running any mapper. Otherwise it tries each mapper in order. If none matches, it falls back to `operation.NewFallbackError(err)`: code `"x"`, `StatusUnknown`, message `"an error occurred"`, `SeverityCritical`.

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
- With `RetryAttempts` 0, retries go on until an attempt succeeds, `RetryIf` refuses its error, or `RetryTimeout` or the operation's context ends. Cron jobs run on a context that never ends, so give them `RetryAttempts` or `RetryTimeout`. Without a `RetryDelay` as well, `Tolerance{Retry: true}` retries a failing operation in a tight loop.
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

On HTTP routes, attach a middleware that reads request parameters with `http.MiddlewareAs`, so the generated API documentation lists them. See [Middlewares that read request parameters](#middlewares-that-read-request-parameters).

### Order

Every server builds the chain as **base middlewares → server middlewares → narrower scopes → handler**. Base middlewares are skipped when `DisableBaseMiddlewares` is true.

| Server | Base chain (outer → inner) | Then |
| --- | --- | --- |
| HTTP | `ID` → `Logger` → `Recorder` (request/response fields) → `ErrorResponder` (writes unwritten errors) → `Recovery` (serialized error) | server → route → handler |
| Cron | `ID` → `Logger` → `Locker` (job lock) → `Recorder` → `Tolerance` (job tolerance) → `Recovery` | server → job |
| Worker consumer | `ID` → `Logger` (+`msg_id`) → `Locker` (per message) → `Recorder` (headers, payload) → `Recovery` (retry/reject) → `Guard` (inbox idempotency) | server → subscriber → handler → bind/transaction/handler |
| CLI | `ID` → `Logger` → `Locker` (command lock) → `Recorder` → `Recovery` | cli → command → parse/validate/run |

The HTTP server also ships opt-in `CORS`, `RateLimit` and `JWTAuth` middlewares. See [Built-in HTTP middlewares](#built-in-http-middlewares).

### Built-in middlewares (`github.com/efegreat/forgo/middleware`)

| Middleware | Behavior |
| --- | --- |
| `ID()` | Keeps the ID the transport set, such as the HTTP `X-Request-ID` header, when it is at most 128 bytes of visible ASCII. Otherwise, and when it is empty, sets a new UUIDv7. |
| `Logger(adder logger.FieldAdder)` | Derives a logger with `op_type`, `op_name`, `op_id` plus fields from `adder`, and stores it in the context. Runs after `ID`. |
| `Recorder(disabled, before, after)` | Logs `operation started` and then `operation completed` (with `elapsed`) or `operation failed` (with `err_code`, `err_severity`, `elapsed`, the error, and a warn/error level by severity). The code and severity are those of the first `operation.Error` in the error's chain. The logged error is the operation's error itself, whose text holds the internal cause of every `operation.Error` in it. `before`/`after` add fields, and run only when their event is logged, so a level above info skips their work; `disabled(ctx)` skips logging. |
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
| `Observer` | | A `transport.IConnObserver` that is told about accept errors, for metrics or alerts. One that also implements `transport.IConnIOObserver` is told about read and write errors as well. See [`transport`](#transport). |
| `Validators` | | Custom validation tags for the default binder. |
| `Binder` | bingo binder | Request binder (`http.IBinder`). |
| `Serializer` | JSON envelope | Response serializer factory (`http.SerializerFactory`). |
| `ErrorMapper` | binding/validation mappers | Maps handler and binding errors to `operation.Error`. |
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
	DisableAPIDoc bool                      // leave the route out of OpenAPI documents; it is served as usual
}
```

A route is registered as `"<Method> /<Version><Path>"`. Method `GET`, version `v1` and path `/users/{id}` become `GET /v1/users/{id}`. With an empty `Version`, the route is served at `<Path>` alone. `RouteConfig.FullPath()` returns the resulting path. Paths use standard `ServeMux` syntax: `{name}` wildcards, `{name...}` for the remainder, `{$}` for an exact match.

A request that no route matches, including a known path with a different method, gets a JSON `404` with `errCode: "nf"`. It runs as the `not_found` operation through the base and server middlewares. A route with an empty `Method` and the path `/` replaces this fallback.

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
- An error that maps to nothing becomes `500` with `errCode: "x"`, `errMsg: "an error occurred"`.
- A panic in the handler or any non-base middleware is recovered and answered the same way. The `ErrorMapper` picks the response, and the logged cause is the `*middleware.PanicError`, with its stack.

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
	Success(result any) error // write the success response; return nil or the write/serialization failure
	Error(err error) error    // write the error response; return the error the operation fails with
}
```

The factory is called for every response, and the transport calls `Success` or `Error` once. A serializer that is called again must not write again. The default serializer returns `http.ErrAlreadyWritten` in that case.

`Error` receives the mapped `operation.Error`. Its `Status`, `Code` and `Message` make up the response. `Error` returns the error that `Recorder` logs: `err` once the response is written, or, when the write fails, `err` wrapped with that failure (`operation.Error.Wrap`), so that the log keeps the code and severity. A `nil` result is taken as `err`, so a serializer that only reports write failures still logs failed requests as failed.

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

`Params` only drives the generated API documentation. The server never binds or validates it, and the middleware still reads the request itself. Keep `Params` in line with what the middleware reads. Its `validate` tags only shape the documented schema. On a server, `http.MiddlewareAs` behaves exactly like `forgo.MiddlewareAs`. Middlewares without parameters keep using `forgo.MiddlewareAs`.

### Built-in HTTP middlewares

`github.com/efegreat/forgo/server/http` ships three opt-in middlewares: `CORS`, `RateLimit` and `JWTAuth`. They are not in the base chain. Each one takes its config and returns a constructor for `forgo.MiddlewareAs`. The config is sanitized, validated and compiled once, when you call `http.CORS(...)`, `http.RateLimit(...)` or `http.JWTAuth(...)`, and every route the constructor is attached to shares the result. An invalid config fails at startup, and dependencies such as a shared rate limiter are resolved by bootstrap:

```go
var HTTP = http.Config{
	ServerConfig: forgo.ServerConfig{
		Middlewares: []forgo.MiddlewareFactory{
			forgo.MiddlewareAs(http.CORS(http.CORSConfig{AllowOrigins: []string{"https://app.example.com"}})),
			forgo.MiddlewareAs(http.RateLimit(http.RateLimitConfig{Limit: 100, Period: time.Minute})),
		},
	},
	Routes: []http.RouteConfig{
		{
			Name: "me", Method: "GET", Path: "/me", Version: "v1",
			Middlewares: []forgo.MiddlewareFactory{
				forgo.MiddlewareAs(http.JWTAuth(http.JWTConfig{
					Keys:     &http.JWKS{URL: "https://idp.example.com/.well-known/jwks.json"},
					Issuer:   "https://idp.example.com/",
					Audience: []string{"shop-api"},
				})),
			},
			Handler: http.HandlerFor((*user.Service).Me),
		},
	},
}
```

Order them `CORS` → `RateLimit` → `JWTAuth`. CORS then answers preflight requests before they are counted or rejected, and rejections from the later middlewares still carry the CORS headers.

#### CORS

```go
func CORS(cfg CORSConfig) func() (operation.Middleware, error)
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
func RateLimit(cfg RateLimitConfig) func() (operation.Middleware, error)
func RateLimitWith(cfg RateLimitConfig) func(IRateLimiter) (operation.Middleware, error)
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

`RateLimit` counts in its own in-process `MemoryRateLimiter`. Attached to the server, the limit covers all routes together, unmatched requests included. Attached to a route, it covers that route alone. Counts are kept per process.

To enforce a limit across instances, provide an `IRateLimiter` backed by a shared store and use `RateLimitWith`. Bootstrap resolves the limiter. [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#ratelimit) ships one on Redis, with the same algorithm as `MemoryRateLimiter`:

```go
type IRateLimiter interface {
	Allow(ctx context.Context, key string, rate Rate) (RateLimitResult, error)
}

forgo.WithResources(redisConf)          // redisdb.Config from forgo-adapter
forgo.WithProviders(ratelimit.NewRedis) // an http.IRateLimiter from github.com/efegreat/forgo-adapter/provider/ratelimit

forgo.MiddlewareAs(http.RateLimitWith(http.RateLimitConfig{Limit: 5, Period: time.Minute, Prefix: "login:"}))
```

`MemoryRateLimiter` implements the generic cell rate algorithm (GCRA) and stores one timestamp per active key. Keys whose budget has fully recovered are swept, and at most `maxKeys` keys are tracked. `http.NewMemoryRateLimiter(maxKeys)` creates one you can share through a provider, where 0 means 1 << 20 keys.

`http.ClientIP(r, trustedProxies)` resolves the client address the same way the default key does. Use it to build your own keys. In this example, `JWTAuth` runs before `RateLimit` so the claims are available:

```go
Key: func(ctx *operation.Context) (string, error) {
	if c, ok := http.JWTClaimsFromCtx(ctx); ok {
		return "user:" + c.Subject, nil
	}
	return "ip:" + http.ClientIP(ctx.State.(*http.State).Request, proxies).String(), nil
},
```

#### JWT authentication

```go
func JWTAuth(cfg JWTConfig) func() (operation.Middleware, error)
```

| Field | Default | Description |
| --- | --- | --- |
| `Keys` | required | `http.JWTKeys` for keys known up front, `*http.JWKS` for a key set URL, or any `http.IJWTKeySet`. |
| `Issuer` | | When set, the `iss` claim must equal it. |
| `Audience` | | When set, the `aud` claim must contain one of these values. |
| `Leeway` | `0` | Clock skew tolerated when `exp` and `nbf` are checked. |
| `Cookie` | | Name of a cookie the token is read from when the request has no bearer token. |
| `Optional` | `false` | Requests without a token proceed, without claims. A token that is sent must still be valid. |

The token is read from `Authorization: Bearer <token>`, then from `Cookie` if it is configured. It is parsed, its signature verified and its claims validated by [golang-jwt/jwt/v5](https://github.com/golang-jwt/jwt). Forgo supplies the keys and the rules:

- **Algorithms:** `HS256`, `HS384`, `HS512`, `RS256`, `RS384`, `RS512`, `PS256`, `PS384`, `PS512`, `ES256`, `ES384`, `ES512` and `EdDSA`. `none` is never accepted.
- **Keys bound to algorithms:** each algorithm accepts only keys of its own type and strength: HMAC secrets at least as long as the hash, RSA keys of at least 2048 bits, ECDSA keys on the algorithm's curve. This defeats algorithm confusion: an RSA public key is never used as an HMAC secret. `JWTKey.Algorithm` pins a key to a single algorithm. Keys from a custom `IJWTKeySet` are held to the same rules.
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
type JWTKey struct {
	ID        string // matched against kid
	Algorithm string // optional pin, such as "RS256"
	Key       any    // []byte (≥ 32 bytes; HS384 ≥ 48, HS512 ≥ 64), *rsa.PublicKey (≥ 2048 bits), *ecdsa.PublicKey (P-256, P-384, P-521) or ed25519.PublicKey
}

Keys: http.JWTKeys{{ID: "2026-10", Algorithm: "HS256", Key: []byte(forgo.Env("JWT_SECRET", ""))}},
```

**Key sets.** `*http.JWKS` fetches an identity provider's JSON Web Key Set.

| Field | Default | Description |
| --- | --- | --- |
| `URL` | required | The provider's `jwks_uri`. |
| `Client` | `http.DefaultClient` | Each fetch is limited to 10 seconds and 1 MiB. |
| `RefreshInterval` | `1h` | Keys older than this are fetched again. |
| `MinRefreshInterval` | `1m` | Minimum time between two fetches. |

Keys are fetched on first use and cached. They are fetched again once they are stale, or when a token names an unknown `kid`, which is how key rotations are picked up. Fetches happen at most once per `MinRefreshInterval`, so tokens with made-up key IDs cannot flood the provider. Requests never wait for a fetch they do not need: while one request refreshes stale keys, the others keep using them. While the provider is down, the keys fetched last stay in use. Only RSA, EC and Ed25519 keys whose `use` is empty or `sig` are loaded.

**Claims.** Handlers and later middlewares read the verified claims with `http.JWTClaimsFromCtx`:

```go
func (s *Service) Me(ctx context.Context, _ MeReq) (MeResp, error) {
	claims, _ := http.JWTClaimsFromCtx(ctx)

	var custom struct {
		Scope string `json:"scope"`
	}
	if err := claims.Decode(&custom); err != nil {
		return MeResp{}, err
	}
	return s.get(ctx, claims.Subject)
}
```

`JWTClaims` has `Issuer`, `Subject`, `Audience`, `ExpiresAt`, `NotBefore`, `IssuedAt` and `ID`. Time claims are whole seconds, and those a token does not have are zero. `Decode(v)` unmarshals the whole payload, for custom claims.

**Authorization.** To check scopes or roles, add a `middleware.Guard` after `JWTAuth`:

```go
func RequireScope(scope string) func() operation.Middleware {
	return func() operation.Middleware {
		return middleware.Guard(
			func(ctx *operation.Context) (bool, error) {
				claims, ok := http.JWTClaimsFromCtx(ctx)
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

Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(http.JWTAuth(jwtConf)), forgo.MiddlewareAs(RequireScope("orders:write"))},
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

`Terminate` calls `http.Server.Shutdown`. The server stops accepting connections and waits for in-flight requests until `TermTimeout`. Combine it with a `TermSig` and an `http.HealthHandler` route for [readiness-aware shutdown](#termination-settings) and [zero-downtime rolling updates on Kubernetes](#zero-downtime-deployments-on-kubernetes).

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
- Retries re-run everything inside the `Tolerance` middleware, including server middlewares. The lock is held across all attempts and is not renewed, so its `TTL` must cover `RetryAttempts × (run time + RetryDelay)`. One `operation started`/`completed` pair is logged per run.
- A run whose lock cannot be acquired, for example because the locker's backend is down, does not run and is logged as `failed to acquire operation lock`.
- Jobs receive a context that is **not** cancelled on shutdown. `Terminate` stops scheduling and waits for running jobs. Give long jobs their own timeouts and size `TermTimeout` accordingly.
- Cron has no per-job middlewares. Use server middlewares, or compose inside the job.

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

| Field | Default | Description |
| --- | --- | --- |
| `ConsumerConfig.ServerConfig` | | Termination, server-wide middlewares. |
| `ConsumerConfig.Binder` | JSON + validator | `worker.IBinder` that decodes `Message` into `Req`. |
| `ConsumerConfig.SensitiveFields` | | Names whose values are masked as `[*]` in the logged headers, payload and binding errors. The credential headers, such as `Authorization`, are always masked. See [Masking sensitive fields](#masking-sensitive-fields). |
| `ConsumerConfig.Clean` | see below | Inbox cleanup. |
| `SubscriberConfig.Name` | | Passed to `IBroker.NewSubscriber`. |
| `SubscriberConfig.Concurrency` | `1` | Messages handled in parallel. |
| `SubscriberConfig.MaxAttempt` | `3` | Attempts before a message is marked failed and rejected. |
| `SubscriberConfig.AckTimeout` | `3s` | Time limit for each acknowledgement sent to the broker: complete, retry or reject. |
| `SubscriberConfig.Middlewares` | | Middlewares for every handler of the subscriber. |
| `HandlerConfig.Name` | | The `Message.Name` the handler receives. |
| `HandlerConfig.Lock` | | Per-message lock. The key becomes `<Key>-<subscriber>-<messageID>`. If another delivery holds it, the message is retried (`msg_locked`, logged as a warning) after the broker's [retry delay](#retry-delays). It is not acknowledged, because the holder may have crashed and left its lock until it expires. |
| `HandlerConfig.TransactionDisabled` | `false` | Skip the `Transactor`. |
| `HandlerConfig.Timeout` | no limit | Ends the context your handler receives. An error the handler returns after that fails with `msg_timeout`, unless it is an `operation.Error`, and is retried like any handler error. Go cannot stop a goroutine: a handler that ignores its context keeps running and holds its concurrency slot. The inbox mark and the commit that follow the handler are not bound by it. |
| `HandlerConfig.Middlewares` | | Handler middlewares. |
| `HandlerConfig.Args` | | Static values in `ctx.Args` of every message. Supports `worker.ArgKeyRecorderDisabled`. |

`New` rejects a negative `Concurrency`, `MaxAttempt`, `HandlerConfig.Timeout`, `Clean.CleanupLimit` or `Clean.CleanupInterval`, which would otherwise only fail while serving.

**Required dependencies:** `worker.IBroker` and `worker.InboxStorage`. `operation.Transactor` is needed unless every handler sets `TransactionDisabled`. Your inbox storage must join that transaction, so that the handler's writes and the *completed* mark commit together. forgo-adapter's `sqltx.Transactor` and SQL inbox storages do. `operation.ILocker` is needed only when a handler sets `Lock`. A missing dependency fails at startup and names the handler that needs it.

A subscriber that stops consuming on its own, for example because its broker gave up reconnecting, makes `Serve` return its error. That stops the app, so that an orchestrator restarts it instead of leaving it up without consuming.

**Processing a message:**

1. The subscriber receives a delivery and picks the handler by `Message.Name`. An unknown name is rejected (no redelivery) and logged with code `nf_mh`.
2. Base middlewares run: `ID`, `Logger` (adds `msg_id`), `Locker`, `Recorder` (logs headers and payload with `SensitiveFields` and the credential headers masked, then `msg_elapsed` since `CreatedAt`), `Recovery`, and the **idempotency guard**. The guard looks the message up in the inbox by `(message ID, subscriber)`. A message already completed, failed or rejected is acknowledged and skipped.
3. Server, subscriber and handler middlewares run.
4. The inbox attempt counter is incremented. A new message is recorded as *waiting*. A message whose count passes `MaxAttempt` is marked *failed* and rejected without running the handler (`msg_attempts`). That only happens when earlier attempts ended without a verdict, for example because the message crashed the process. Otherwise such a message would crash every consumer it reaches, forever.
5. The payload is bound into `Req`. On failure the message is marked *rejected* and rejected at the broker, with no retry (`msg_bind`).
6. `Transactor.Do` runs your handler, then marks the inbox row *completed*. Both run on the transaction's context. With `HandlerConfig.Timeout`, the handler's context ends after it.
7. **Success:** the delivery is acknowledged. **Error or panic:** while attempts are below `MaxAttempt`, the delivery is retried, after the broker's [retry delay](#retry-delays) for the failed attempt. At `MaxAttempt` it is marked *failed* and rejected.

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

A message that reaches `MaxAttempt` is marked *failed* and rejected, so it moves to the dead-letter queue if its queue has one. Its inbox row stays *failed*, and the idempotency guard acknowledges and skips every later delivery of it. Retrying it is a manual decision, done in this order:

1. Set the inbox row back to *waiting* with zero attempts.
2. Replay the message from the dead-letter queue.

```sql
-- forgo-adapter's inbox_events table. Narrow it down with message_id to retry single messages.
UPDATE inbox_events
SET status = 0, attempts = 0, updated_at = CURRENT_TIMESTAMP -- 0: InboxWaiting
WHERE subscriber_name = 'billing'                             -- SubscriberConfig.Name
  AND status = 2;                                             -- 2: InboxFailed
```

Do not reverse the order: a message replayed onto a row that is still *failed* is acknowledged and dropped. Reset `attempts` as well as `status`. Otherwise the replay exceeds `MaxAttempt` and fails at once with `msg_attempts`, without running the handler. *Rejected* messages (status 3, payloads that failed to bind) are retried the same way once the binding is fixed.

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
| `ClaimTTL` | `1m` | How long a claim belongs to one producer instance. Older claims can be taken over, for example after a crash. A batch stops publishing at half its `ClaimTTL`, whatever the publishers' `Timeout`. Messages not published by then are released for the next batch, so another instance never takes over a claim that is still being published. |
| `DefaultPublisher` | name `default`, concurrency `1`, timeout `3s` | Publishes messages whose `Name` matches no entry in `Publishers`. Its broker publishers come from `IBroker.NewDefaultPublisher`; `Name` only labels its logs. |
| `Publishers` | | Dedicated publishers. A message goes to the publisher whose `Name` equals `Message.Name`; the same name is passed to `IBroker.NewPublisher`. `Concurrency` is the number of broker publishers, `Timeout` the per-publish timeout. |
| `ErrorPercentage`, `OnErrorPercentage` | off | When a batch's failure rate reaches `ErrorPercentage`, `OnErrorPercentage` is called. Without a callback, the breach is logged with `err_pct`. |
| `Clean` | see below | Deletion of processed outbox rows. |

`New` rejects a negative `Limit`, `Interval`, `ClaimTTL`, publisher `Concurrency` or `Timeout`, `Clean.CleanupLimit` or `Clean.CleanupInterval`, which would otherwise only fail while serving.

**Required dependencies:** `worker.IBroker` and `worker.OutboxStorage`.

Each cycle **claims** up to `Limit` unprocessed messages, publishes them in parallel, then **completes** the claim. Published messages are marked processed, and failed ones are released for the next cycle. Delivery is *at least once*; the consumer's inbox makes handling idempotent. When `Claim` or `Complete` fails, the batch is logged (`outbox batch failed`) and retried after `Interval`, so a storage outage pauses the relay instead of stopping it.

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

The SQL outbox and inbox storages of [forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#inbox-and-outbox) (`inbox.NewPostgres`, `inbox.NewMysql`, `outbox.NewPostgres`, `outbox.NewMysql`) work on these tables. A PostgreSQL example:

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
	// attempt is the failed attempt (0 before it was counted): the broker derives its retry delay from it.
	Retry(ctx context.Context, attempt int) error // nack, redeliver
	Reject(ctx context.Context) error             // drop, no redelivery
}
```

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
				Args:    new(types.Map[any, any]),
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

The CLI runs **one command per invocation**, with the same resources, providers and middleware model as an App. Where an App runs servers until it is stopped, a CLI bootstraps, runs a single command as an operation, waits for its background goroutines, and returns.

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
				{Name: "migrate", Command: migration.Factory(migration.CommandConfig{})},
				{Name: "openapi", Command: cli.As(openapi.NewDefaultOpenAPICommand(config.HTTP))},
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
5. Resolves the optional `operation.ILocker`, the global and command middlewares, and the command constructor. Then seals the bootstrap.
6. Runs the command as an operation (`Type: "command"`, `Name: <command>`) through `ID` → `Logger` → `Locker` → `Recorder` → `Recovery`, then the global middlewares, then the command middlewares.
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
{Name: "openapi", Command: cli.As(openapi.NewDefaultOpenAPICommand(config.HTTP))}
```

With options:

```go
func NewOpenAPICommand() cli.CommandFactory {
	return func() cli.Command {
		return openapi.NewCommand(config.HTTP,
			openapi.WithInfo(openapi.Info{
				Title:       "Shop API",
				Description: "Public HTTP API of the shop",
				Version:     "1.4.0", // the version of the unversioned document
			}),
			openapi.WithSchema[shop.Money](openapi.Schema{"type": "string", "format": "decimal"}),
			openapi.WithDocument(func(version string, doc *openapi.Document) error {
				doc.Components.SecuritySchemes = map[string]openapi.SecurityScheme{
					"bearer": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
				}
				doc.Security = []openapi.SecurityRequirement{{"bearer": {}}}
				return nil
			}),
		)
	}
}
```

| Option | Default | Effect |
| --- | --- | --- |
| `WithInfo(Info)` | `API Doc`, `HTTP API Doc`, `1.0.0` | Title, description, and the version of the document of unversioned routes. Versioned documents take the route version. An empty title or version gets the default. |
| `WithSchema[T](Schema)` | | Documents `T` as the given schema in bodies, responses and parameters. Use it for types whose JSON shape cannot be inferred (their own `json.Marshaler`/`json.Unmarshaler`) and to give named types a `format` or `enum`. `validate` constraints still apply around it. |
| `WithDocument(hook)` | | Runs `hook(version, doc)` on every generated document, in version order, to add what routes do not declare: `Servers`, `Security`, `Components.SecuritySchemes`, `Tags`, and per operation (found with `doc.Operation(routeName)`) `Tags`, `Summary`, `Deprecated` or `Security`. An empty, non-nil `Operation.Security` documents an operation without authentication. A hook error fails generation. |

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
- **Middleware parameters** from the `Params` of every server and route middleware attached with `http.MiddlewareAs` (see [Middlewares that read request parameters](#middlewares-that-read-request-parameters)):
  - `Params` takes `header`, `query` and `uri` fields only. A `json` field fails generation, because the body belongs to the handler.
  - A `uri` field must exist in the path of every route the middleware runs on.
  - A parameter declared by several middlewares or by the handler is documented once, as required when any declaration requires it. Declarations with different schemas fail generation.
  - To read `Params`, the generator calls each middleware factory with a resolver that provides nothing of the application. `http.MiddlewareAs` factories report their `Params` there without calling their constructor. Other factories are ignored: they fail to resolve their dependencies, or construct a middleware that is discarded.
- **Validation hints** from `validate` tags: `required`, `min`, `max`, `len`, `gt`, `gte`, `lt`, `lte`, `oneof`, `email`, `uuid*`, `hostname`, `hostname_rfc1123`, `url`, `uri`, `ip`, `ipv4` and `ipv6` become schema constraints. Rules after `dive` or `keys` are not applied to the collection itself.
- **Response schema** from the response type, placed inside your actual serializer's envelope. The generator probes the configured `Serializer` (default or custom), finds where the result lands, and copies the status, media type and response headers it observes.
- A `default` error response, from probing the serializer's `Error`.
- `EmptyHandler` and `HealthHandler` routes have no request and no result.
- `LiveConfigsHandler` routes have a result whose config values have no schema. `SetLiveConfigHandler` routes have their `name` and `If-Match` parameters, but no body, because it depends on the config.

**Supported types:** primitives, pointers (nullable), slices, arrays, maps with JSON-compatible keys, nested and recursive structs (as `components/schemas`), embedded structs with JSON dominance rules, `time.Time`, `json.Number`, `json.RawMessage`, and text types: `encoding.TextUnmarshaler` in requests and `encoding.TextMarshaler` in responses. A type with its own JSON encoding (`json.Unmarshaler` in a request, `json.Marshaler` in a response) fails generation, because its shape cannot be inferred, unless it has a `WithSchema`.

**Component names** follow the Go type: `Product`, and `PageProduct` for `Page[shop.Product]`. The request schema of a type also used in a response, which keeps only `json` fields and carries validation, is `ProductRequest`. Colliding names get the package name as a prefix (`ShopProduct`), then a hash suffix if that is not enough. Names do not depend on route order.

**Programmatic use:**

```go
gen := openapi.New(config.HTTP, openapi.WithInfo(openapi.Info{Title: "Shop API"})) // New sanitizes the config
docs, err := gen.GenerateAll(ctx)                                                  // map[version]*openapi.Document
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
| `op_type` | `OperationTypeLogKey` | Every line of an operation | `http_handler`, `cron_job`, `message_handler`, `command` |
| `op_name` | `OperationNameLogKey` | Every line of an operation | The route, job, message handler or command name |
| `op_id` | `OperationIDLogKey` | Every line of an operation | The request's `X-Request-ID` when it is at most 128 bytes of visible ASCII, otherwise a new UUIDv7 |
| `elapsed` | `ElapsedTimeLogKey` | `operation completed`, `operation failed` | Seconds, as a float |
| `err_code` | `ErrorCodeLogKey` | `operation failed` | `operation.Error.Code` |
| `err_severity` | `ErrorSeverityLogKey` | Every error line | `0` light, `1` moderate, `2` critical |
| `cron_job`, `schedule` | `CronJobLogKey`, `CronScheduleLogKey` | `cron job added` | The job's name and schedule |
| `kind` | `KindLogKey` | HTTP `operation started` and end lines | `request`, `response` |
| `addr`, `method`, `path`, `query` | `RequestAddrLogKey`, `RequestMethodLogKey`, `RequestPathLogKey`, `RequestQueryLogKey` | HTTP `operation started` | The client address (`RemoteAddr`), method, path and raw query, masked |
| `req_headers`, `req_body` | `RequestHeadersLogKey`, `RequestBodyLogKey` | HTTP `operation started` | A JSON object; the body as JSON or a string |
| `read_err` | `RequestBodyReadErrLogKey` | HTTP `operation started`, when reading the body failed | The error |
| `resp_status` | `ResponseStatusLogKey` | HTTP end lines | The status code, as an integer |
| `resp_headers`, `resp_body` | `ResponseHeadersLogKey`, `ResponseBodyLogKey` | HTTP end lines | As for the request |
| `headers_cut`, `body_cut` | `HeadersCutLogKey`, `BodyCutLogKey` | Next to logged headers, bodies and payloads | `true` when they were cut at the logging limit, or dropped |
| `msg_id` | `MessageIDLogKey` | Every line of a consumed message | `Message.ID` |
| `msg_headers`, `msg_payload` | `MessageHeadersLogKey`, `MessagePayloadLogKey` | Consumer `operation started` | A JSON object; the payload as JSON or a string |
| `msg_elapsed` | `MessageElapsedLogKey` | Consumer end lines | Seconds since `Message.CreatedAt`, as a float |
| `broker`, `sub`, `sub_id` | `BrokerLogKey`, `SubscriberLogKey`, `SubscriberIDLogKey` | Every line of a consumer's subscriber | The broker and subscriber names, and an xid |
| `pub`, `pub_id` | `PublisherLogKey`, `PublisherIDLogKey` | Every line of a producer's publisher, with `broker` | The publisher name, and an xid |
| `err_pct` | `ErrorPercentageLogKey` | `publish error percentage reached` | The failure rate of the batch, in percent |
| `live_config` | `LiveConfigLogKey` | Live config cache failures | The config's name |

### Masking sensitive fields

The HTTP server and the worker consumer log what passes through them, so values such as passwords and tokens must not reach the logs as sent. Name them in the `SensitiveFields` of `http.Config` or `worker.ConsumerConfig`, and every value logged under one of those names becomes `[*]`:

| Where | HTTP server | Worker consumer |
| --- | --- | --- |
| Headers | Request and response headers. | Message headers. |
| Query | Parameters, names unescaped first: `?token=abc&page=2` logs `token=[*]&page=2`. | |
| Path | Wildcards of the route pattern: `/reset/{token}` logs `/reset/[*]`. A `{name...}` wildcard masks the rest of the path. | |
| JSON body | Request and response bodies, at any depth. The whole value of a sensitive member is masked, objects and arrays included. | The payload, the same way. |
| Binding errors | The logged cause of a field that fails to bind. | The same, for payload fields. |

Names match case-insensitively, the way `encoding/json` matches keys to struct fields: `password` masks `"Password"`, `"PASSWORD"` and the escaped `"pass\u0077ord"`. One list serves every place, so a name masks a header, a query parameter, a wildcard and a body member alike. Both servers also always mask the credential headers `Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie` and `X-Api-Key`.

Masking changes only what is logged. Handlers and clients still see every value as sent.

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

Implement both methods on the value receiver. The configuration is stored as the JSON encoding of `T`, so give its fields json tags.

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

`Load` never writes the shared cache: only `Save` does. A config that is stored but not cached, for example after the cache was flushed, is read from the store on every `Load` until it is saved again. Every call returns a new value, so callers may modify it freely.

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

forgo-adapter provides the SQL stores `liveconfig.NewPostgres` and `liveconfig.NewMysql`, and the Redis cache `liveconfig.NewRedisCache` (see its [REFERENCE.md](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md#liveconfig)). The SQL stores use this table:

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
);
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
for k, v := range m.Range {                     // or m.Range(func(k string, v int) bool { ... })
	fmt.Println(k, v)
}

n := m.GetTyped[int]("a")                       // typed read; zero value if absent or mismatched
```

`GetTyped[T]` also converts `json.Number` values to sized integer and float types without losing precision.

`Range` holds the read lock for the whole iteration, so it sees a consistent view without copying the map. Inside the loop, don't call methods of the same map: a write deadlocks, and so can a read while a writer waits. Use `Clone` when the loop body needs the map.

The zero `Map` is empty and ready to use, and lookups in an empty map skip the lock. `types.NewMapCopyOnWrite(data)` reads `data` in place and clones it before its first write. Use it for static data shared by many maps, such as route args, and never modify `data` afterwards.

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
| `transport.IConnObserver` | `OnAcceptError`, called for every failed `Accept` except after the listener is closed. |
| `transport.IConnIOObserver` | An `IConnObserver` with `OnReadError` and `OnWriteError` too. |
| `transport.Listener` | A `net.Listener` that reports to its `Observer`, and reports nothing when `Observer` is nil. |

Observers are for metrics and alerts. Accept errors are already logged: net/http logs the ones it retries through `ErrorLog`, and `App` logs the one `Serve` returns. Hooks run concurrently, on the accept loop and on the connections' goroutines, so they must not block.

The listener wraps accepted connections only for an `IConnIOObserver`. A wrapper hides the zero-copy paths of `*net.TCPConn`, so the other observers do not pay for it. A wrapped connection forwards `CloseWrite`. It also has `NetConn()`, which returns the accepted connection, like `*tls.Conn` does. `ConnState` and `ConnContext` hooks can use it to reach the `*net.TCPConn`. The read and write hooks receive the accepted connection and report failures only:

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
| `sqldb` | `github.com/efegreat/forgo-adapter/resource/sqldb` | resource | `*sql.DB` | Applies pool limits and pings on startup. Import your SQL driver in `main`. |
| `redisdb` | `github.com/efegreat/forgo-adapter/resource/redisdb` | resource | `redis.UniversalClient` | One server, a Redis Cluster or a Sentinel-managed master, chosen from `Addrs`, `MasterName` and `ClusterMode` like `redis.NewUniversalClient`. `ReadFrom` sends read-only commands to replicas. Pings on startup. The Redis providers below request `redis.UniversalClient`. |
| `rabbitmq` | `github.com/efegreat/forgo-adapter/resource/broker/rabbitmq` | resource | `worker.IBroker` | Declares exchanges, queues and bindings at startup. Named publishers and subscribers match worker config names. Reconnects automatically, waits for publisher confirms, and sets the consumer prefetch to the subscriber's `Concurrency` by default. Optional per-queue dead-letter queues (`DeadLetter`) keep the messages the consumer rejects, and retry queues (`Retry`) delay redeliveries exponentially. |
| `pubsub` | `github.com/efegreat/forgo-adapter/resource/broker/pubsub` | resource | `worker.IBroker` | Google Cloud Pub/Sub. Creates the listed topics and subscriptions at startup and checks the settings of existing subscriptions. Messages go to the topic named by `Destination`, with `Key` as their ordering key, and publishers share one batching client publisher per topic. The number of messages a subscriber holds defaults to the subscriber's `Concurrency`. Optional subscription retry policies (`Retry`) delay redeliveries, and dead-letter topics (`DeadLetter`) keep the messages the consumer rejects. |
| `redisstream` | `github.com/efegreat/forgo-adapter/resource/broker/redisstream` | resource | `worker.IBroker` | Redis Streams, over a client of its own built from a `redisdb.Config`. Creates the listed consumer groups at startup. Messages go to the stream named by `Destination`, optionally trimmed (`MaxLen`). Each subscriber reads its group as a consumer of its own, no more entries than it has free slots, keeps the entries it handles from being claimed, and claims the ones a stopped consumer left pending for `ClaimIdle`. Optional delayed retries (`Retry`) and dead-letter streams (`DeadLetter`). |
| `sqltx` | `github.com/efegreat/forgo-adapter/provider/sqltx` | provider | `operation.Transactor` | Carries a `database/sql` transaction in the context. `sqltx.From(ctx, db)` joins it from any repository or storage. |
| `inbox` (Postgres, MySQL) | `github.com/efegreat/forgo-adapter/provider/inbox` | provider | `worker.InboxStorage` | Uses the `inbox_events` table. `MarkCompleted` succeeds only for the delivery that completes the message, so a concurrent duplicate rolls back its handler's transaction. |
| `outbox` (Postgres, MySQL) | `github.com/efegreat/forgo-adapter/provider/outbox` | provider | `worker.OutboxStorage` | Uses the `outbox_events` table. Claims with `FOR UPDATE SKIP LOCKED`. |
| `locker` (Redis) | `github.com/efegreat/forgo-adapter/provider/locker` | provider | `operation.ILocker` | `SET NX` with TTL. Unlock only releases a lock this process still holds. |
| `ratelimit` (Redis) | `github.com/efegreat/forgo-adapter/provider/ratelimit` | provider | `http.IRateLimiter` | GCRA in one Lua script on the Redis clock, for `http.RateLimitWith` across instances. |
| `liveconfig` (Postgres, MySQL, Redis) | `github.com/efegreat/forgo-adapter/provider/liveconfig` | provider | `forgo.LiveConfigStore`, `forgo.LiveConfigCache` | SQL stores on the `live_configs` table (see [Live configs](#live-configs)) with versioned conditional saves, outside any transaction the context carries. A Redis cache whose entries never expire and keep the newest version. |

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
| `x` | any | Unknown → HTTP 500 | Unmapped error (`operation.NewFallbackError`) |
| `nf` | HTTP | 404 | No route matches the request |
| `terminating` | HTTP `HealthHandler` | 503 | The server's `TermSig` is signaled |
| `live_config_not_found` | HTTP `SetLiveConfigHandler` | 404 | The catalog has no config of that name |
| `live_config_version` | HTTP `SetLiveConfigHandler` | 400 | `If-Match` is missing or is not a version |
| `live_config_invalid` | HTTP `SetLiveConfigHandler` | 400 | The body is not a JSON object of the config's fields, or is larger than 1 MiB |
| `live_config_conflict` | HTTP `SetLiveConfigHandler` | 412 | The config was changed since the version in `If-Match` |
| `live_config_not_cached` | HTTP `SetLiveConfigHandler` | 503 | The change is stored, but the cache missed it; send it again |
| `eof` | HTTP | 400 | Request body is empty or truncated |
| `json_syntax` | HTTP | 400 | Malformed JSON body |
| `json_type` | HTTP | 400 | JSON value has the wrong type |
| `binding` | HTTP | 400 | Query, path or header value could not be bound |
| `validation` | HTTP | 400 | `validate` rule failed |
| `unauthenticated` | HTTP `JWTAuth` | 401 | The request has no bearer token |
| `invalid_token` | HTTP `JWTAuth` | 401 | The token cannot be verified or its claims are not accepted |
| `token_expired` | HTTP `JWTAuth` | 401 | The token is valid but expired |
| `auth_unavailable` | HTTP `JWTAuth` | 503 | The keys to verify the token cannot be fetched |
| `rate_limited` | HTTP `RateLimit` | 429 | The client exceeded the rate |
| `rate_limit_unavailable` | HTTP `RateLimit` | 503 | The rate limiter failed and `FailOpen` is off |
| `rejected` | `middleware.Guard` | PermissionDenied → HTTP 403 | A `Guard` without a `reject` function denied the operation (`middleware.ErrRejected`) |
| `msg_bind` | worker | InvalidArgument | Payload could not be decoded or validated; message rejected |
| `msg_ack` | worker | Internal | Acknowledging the delivery failed |
| `nf_mh` | worker | InvalidArgument | No handler for the message name; message rejected |
| `command_parse` | CLI | InvalidArgument | Command flags could not be parsed |
| `command_validate` | CLI | InvalidArgument | `Command.Validate` failed |
| `command_locked` | CLI | Aborted | Another run holds the command's `Lock`; the command did not run |

Bootstrap errors are returned or panicked before anything serves. They start with `bootstrap:` and name the provider, argument and type involved. Check them with `errors.Is` against `forgo.ErrMissingDependency`, `forgo.ErrAmbiguousDependency` and `forgo.ErrResourceNotFound`.
