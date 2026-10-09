# forgo

**The Go framework for building complete backend systems.**

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
app.Run()
```

## Why forgo

- **Simplicity.** Resources, servers, routes, jobs, message handlers and commands are all declared as configuration. You hand over constructors and forgo wires them.
- **Speed.** The dependency graph is resolved and sealed before the first request arrives. Nothing is looked up or reflected while the application runs.
- **Abstraction.** HTTP requests, cron runs, consumed messages and CLI commands are all *operations*. A middleware written once works on every server.
- **Scalability.** The architecture is clean and replaceable. Servers, brokers, storages, binders, serializers and lockers all sit behind interfaces you can swap or extend.

## What's in the box

| | |
| --- | --- |
| **Bootstrap** | Dependency resolver that wires resources and providers before any server starts. Dependencies are dynamic while the app is being constructed and static once it runs. |
| **App lifecycle** | Runs many servers as one app and manages start, graceful termination with readiness signaling, and forced termination. |
| **Servers** | Built-in **HTTP**, **Cron** and **Worker** (broker-agnostic consumer with an idempotent inbox, plus a transactional outbox producer). Implement one interface to add your own. |
| **CLI** | Define commands and invoke them from the command line, with the same dependency model as an app. |
| **Built-in commands** | SQL migrations for PostgreSQL and MySQL, and OpenAPI 3.1 generation straight from your HTTP route definitions. |
| **Operations and middleware** | One transport-agnostic execution model with reusable middlewares: recovery, logging, recording, retries, distributed locks, guards. Opt-in HTTP middlewares for CORS, rate limiting and JWT authentication. |
| **Logging** | Structured, zero-allocation logging via zerolog. Every line carries app, server and operation fields for seamless tracing. |
| **`process.Go`** | Tracked background goroutines the app waits for before it exits. |
| **Adapters** | Ready-made resources and providers: SQL databases, RabbitMQ, outbox/inbox storage, Redis locks. |
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

## Requirements

Go 1.27 or newer.

## Documentation

- **[REFERENCE.md](REFERENCE.md)** is the complete usage guide: bootstrap rules, app lifecycle, every server, the CLI and built-in commands, operations, middleware, logging, background goroutines and adapters.
  - [Quick start](REFERENCE.md#quick-start)
  - [Bootstrap](REFERENCE.md#bootstrap)
  - [Running multiple servers, or deploying them separately](REFERENCE.md#running-several-servers-in-one-app)
  - [HTTP](REFERENCE.md#http-server) · [Cron](REFERENCE.md#cron-server) · [Worker](REFERENCE.md#worker) · [Custom servers](REFERENCE.md#custom-servers)
  - [CLI](REFERENCE.md#cli) · [Built-in commands](REFERENCE.md#built-in-commands)
- **[AGENTS.md](AGENTS.md)** is the guide for contributors and coding agents working on forgo itself.
