# forgo

**The Go framework for building complete backend systems.**

[![Go Reference](https://pkg.go.dev/badge/github.com/efegreat/forgo.svg)](https://pkg.go.dev/github.com/efegreat/forgo)
[![CI](https://github.com/efegreat/forgo/actions/workflows/ci.yml/badge.svg)](https://github.com/efegreat/forgo/actions/workflows/ci.yml)
[![Go 1.27+](https://img.shields.io/badge/go-1.27%2B-00ADD8)](https://go.dev/dl/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Every backend needs the same technical scaffold before it can do anything useful: dependency wiring, servers, graceful shutdown, traceable logs, retries, locks, migrations, API docs. Forgo implements all of it. You describe your system as configuration and write only the business logic.

```go
app := forgo.NewApp(
	forgo.WithName("users-api"),
	forgo.WithLogger(loggerConf),
	forgo.WithResources(sqlDBConf),            // *sql.DB, opened and closed for you
	forgo.WithProviders(
		postgresrepo.NewUserRepository,        // needs *sql.DB → wired by bootstrap
		user.NewService,                       // needs user.Repository → wired by bootstrap
	),
	forgo.WithServers(httpConf, cronConf),     // run together, shut down together
)
if err := app.Run(); err != nil {              // logged; main picks the exit code
	os.Exit(1)
}
```

## Why forgo

- **Simplicity.** Resources, servers, routes, jobs, message handlers and commands are all declared as configuration. You hand over constructors and forgo wires them.
- **Speed.** The dependency graph is resolved and sealed before the first request arrives. Nothing is looked up or reflected while the application runs.
- **Abstraction.** HTTP requests, gRPC calls, cron runs, consumed messages and CLI commands are all *operations*. A middleware written once works on every server.
- **Scalability.** The architecture is clean and replaceable. Servers, brokers, storages, binders, serializers and lockers all sit behind interfaces you can swap or extend.

## What's in the box

| | |
| --- | --- |
| **Bootstrap** | Dependency resolver that wires resources and providers before any server starts. Dependencies are dynamic while the app is being constructed and static once it runs. |
| **App lifecycle** | Runs many servers as one app and manages start, graceful termination with readiness signaling, and forced termination. A built-in readiness handler makes Kubernetes rolling updates [zero-downtime](REFERENCE.md#zero-downtime-deployments-on-kubernetes), with no `preStop` hooks or shutdown code. |
| **Servers** | Built-in **HTTP**, **gRPC** (handlers take plain structs, so one business method serves an HTTP route and a gRPC method; all four streaming kinds, grpc-go interceptors as middlewares, health checking and reflection), **Cron** and **Worker** (broker-agnostic consumer with an idempotent inbox, plus a transactional outbox producer). Implement one interface to add your own. |
| **CLI** | Define commands and invoke them from the command line, with the same dependency model as an app. |
| **Built-in commands** | SQL migrations for PostgreSQL and MySQL, and OpenAPI 3.1 generation straight from your HTTP route definitions. |
| **Operations and middleware** | One transport-agnostic execution model with reusable middlewares: recovery, logging, recording, retries, distributed locks, guards. Opt-in middlewares for rate limiting and JWT authentication on HTTP and gRPC alike, and CORS. |
| **Logging** | Structured, zero-allocation logging via zerolog. Every line carries app, server and operation fields for seamless tracing. |
| **Metrics** | One observer contract measures every operation of every server, panics included, once the HTTP status or gRPC code is final, with labels that mean the same on every transport. Core has no metrics dependency: forgo-adapter's OpenTelemetry observer exports over OTLP or to Prometheus, or you write one for your backend. [More](REFERENCE.md#metrics). |
| **Live configs** | Typed configuration that admins change while the app runs, such as feature flags and limits. A struct with a name and defaults is read from a cache shared by every instance, then from its store, or from memory when the config allows a short delay. Saves are version-checked, so concurrent edits never overwrite each other, and two ready-made admin endpoints read and change them. [More](REFERENCE.md#live-configs). |
| **`process.Go`** | Tracked background goroutines the app waits for before it exits. |
| **Adapters** | Ready-made resources and providers in the separate [forgo-adapter](https://github.com/efegreat/forgo-adapter) library: SQL databases, Redis, RabbitMQ, Google Cloud Pub/Sub and Redis Streams with dead-letter queues, outbox/inbox storage, live config storage, Redis locks, Redis rate limiting and OpenTelemetry metrics. |
| **Scaffold project** | An example project layout to get you started. |

## A taste

```go
var httpConf = http.Config{
	Routes: []http.RouteConfig{
		{
			Name:    "health",
			Method:  "GET",
			Path:    "/health",
			Version: "v1",
			Handler: http.EmptyHandler(),
		},
		{
			Name:        "getUser",
			Method:      "GET",
			Path:        "/users/{id}",
			Version:     "v1",
			Middlewares: []forgo.MiddlewareFactory{forgo.MiddlewareAs(NewAuthMiddleware)},
			Handler:     http.HandlerFor((*user.Service).Get), // func(ctx, GetReq) (GetResp, error)
		},
	},
}

var cronConf = cron.Config{
	Jobs: []cron.JobConfig{
		{Name: "expireCarts", Schedule: "*/5 * * * *", Job: cron.As(cart.NewExpireJob)},
	},
}
```

```console
$ curl localhost:8080/v1/users/42
{"reqId":"0199…","status":200,"result":{"id":"42","email":"ada@example.com"}}
```

Request binding, validation, error mapping, request IDs, request logging, panic recovery and graceful shutdown are already handled.

## Installation

forgo requires Go 1.27 or newer.

```sh
go get github.com/efegreat/forgo
```

The ready-made adapters are a separate module: `go get github.com/efegreat/forgo-adapter`.

## Documentation

- **[REFERENCE.md](REFERENCE.md)** is the complete usage guide: bootstrap rules, app lifecycle, every server, the CLI and built-in commands, operations, middleware, logging, background goroutines, live configs and adapters.
  - [Quick start](REFERENCE.md#quick-start)
  - [Bootstrap](REFERENCE.md#bootstrap)
  - [Running multiple servers, or deploying them separately](REFERENCE.md#running-several-servers-in-one-app)
  - [Zero-downtime deployments on Kubernetes](REFERENCE.md#zero-downtime-deployments-on-kubernetes)
  - [HTTP](REFERENCE.md#http-server) · [gRPC](REFERENCE.md#grpc-server) · [Cron](REFERENCE.md#cron-server) · [Worker](REFERENCE.md#worker) · [Custom servers](REFERENCE.md#custom-servers)
  - [CLI](REFERENCE.md#cli) · [Built-in commands](REFERENCE.md#built-in-commands)
- **[pkg.go.dev](https://pkg.go.dev/github.com/efegreat/forgo)** has the API documentation of every package, with examples.
- **[AGENTS.md](AGENTS.md)** is the guide for contributors and coding agents working on forgo itself.
- **[forgo-adapter](https://github.com/efegreat/forgo-adapter/blob/main/README.md)** is the companion library of ready-made resources and providers. Its [REFERENCE.md](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md) documents every adapter.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and report vulnerabilities privately as [SECURITY.md](SECURITY.md) describes.

## License

forgo is released under the [MIT License](LICENSE).
