<p align="center">
  <img src="forgo.jpeg" alt="A gopher forging an app on a dwarven anvil" width="420">
</p>

<h1 align="center">forgo</h1>

<p align="center">
  <strong>Your complete backend ecosystem for Go.</strong><br>
  Declare the system. Write the business. forgo forges the rest.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/efegreat/forgo"><img src="https://pkg.go.dev/badge/github.com/efegreat/forgo.svg" alt="Go Reference"></a>
  <a href="https://github.com/efegreat/forgo/actions/workflows/ci.yml"><img src="https://github.com/efegreat/forgo/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://go.dev/dl/"><img src="https://img.shields.io/badge/go-1.27%2B-00ADD8" alt="Go 1.27+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue" alt="License: MIT"></a>
</p>

<p align="center">
  <a href="REFERENCE.md#quick-start">Quick start</a> ·
  <a href="REFERENCE.md">Reference</a> ·
  <a href="https://github.com/efegreat/forgo-adapter">Adapters</a> ·
  <a href="https://github.com/efegreat/forgo-scaffold">Scaffold</a> ·
  <a href="PERFORMANCE.md">Performance</a>
</p>

---

Every backend rebuilds the same scaffold before it ships its first feature: dependency wiring, servers, graceful shutdown, request IDs, structured logs, retries, locks, an outbox, migrations, API docs, and tests that boot the real thing. **forgo ships all of it, production-grade, so the only code you write is the code that makes your product yours.**

```go
func main() {
	app := forgo.NewApp(
		forgo.WithName("users"),
		forgo.WithResources(config.DB()),                         // *sql.DB, opened at start, closed at exit
		forgo.WithProviders(postgres.NewUsers, user.NewService),  // wired by type, checked before serving
		forgo.WithServers(config.HTTP, config.GRPC, config.Cron), // one app, one graceful stop
	)
	if err := app.Run(); err != nil { // every failure is logged; main picks the exit code
		os.Exit(1)
	}
}
```

## Why forgo

Four promises, and every feature keeps all four.

**Simple.** Your system is configuration. Servers, routes, jobs, message handlers and commands are plain Go values, with a default for every setting you leave out. Hand forgo your constructors and it wires them by type: no registration calls, no wiring code to generate, no imposed layout. [Bootstrap →](REFERENCE.md#bootstrap)

**Fast.** The work happens once, at startup. Reflection, dependency lookup and chain building all run before the first request, then the bootstrap is sealed: requests run pre-built closures and log without allocating. A wiring mistake fails at startup, never on a request. [Benchmarks →](PERFORMANCE.md)

**One abstraction.** An HTTP request, a gRPC call, a cron run, a consumed message and a CLI command are all *operations*. Write a middleware once and it runs on every server, and every server logs and measures alike. [Operations →](REFERENCE.md#operations)

**Scalable.** Every part sits behind an interface: servers, brokers, storages, binders, serializers, lockers, observers. Run every server in one binary or each in its own deployment, and replace any part without forking. [Deploying →](REFERENCE.md#deploying-servers-separately)

## A taste

Business code is plain Go. It never imports a transport:

```go
type GetReq struct {
	ID string `uri:"id" validate:"required"` // the path over HTTP, the id field over gRPC
}

func (s *Service) Get(ctx context.Context, req GetReq) (GetResp, error) { … }
```

The rest is configuration, and one method serves both an HTTP route and a gRPC method:

```go
var HTTP = http.Config{
	Routes: []http.RouteConfig{{
		Name: "getUser", Method: "GET", Path: "/users/{id}", Version: "v1",
		Middlewares: []forgo.MiddlewareFactory{http.JWTAuth(Auth)},
		Handler:     http.HandlerFor((*user.Service).Get),
	}},
}

var GRPC = fgrpc.Config{
	Reflection: true,
	Services: []fgrpc.ServiceConfig{{
		Desc:    &userpb.UserService_ServiceDesc,
		Methods: []fgrpc.MethodConfig{{Method: "GetUser", Handler: fgrpc.HandlerFor((*user.Service).Get)}},
	}},
}

var Cron = cron.Config{
	Jobs: []cron.JobConfig{{Name: "expireCarts", Schedule: "*/5 * * * *", Job: cron.As(cart.NewExpireJob)}},
}
```

```console
$ curl -H "Authorization: Bearer $TOKEN" localhost:8080/v1/users/42
{"reqId":"0199…","status":200,"result":{"id":"42","email":"ada@example.com"}}

$ grpcurl -plaintext -d '{"id":"42"}' localhost:9090 user.v1.UserService/GetUser
{
  "id": "42",
  "email": "ada@example.com"
}
```

None of this is yours to write: binding and validation, JWT verification, error mapping to HTTP statuses and gRPC codes, request IDs, request logs with secrets masked, panic recovery, graceful shutdown, and an [OpenAPI 3.1 document](REFERENCE.md#openapi-api-documents) generated from the routes.

## What's in the box

| | |
| --- | --- |
| **Servers** | [HTTP](REFERENCE.md#http-server) on `net/http`, [gRPC](REFERENCE.md#grpc-server) on grpc-go, [cron](REFERENCE.md#cron-server) and a broker-agnostic [worker](REFERENCE.md#worker), or [your own](REFERENCE.md#custom-servers) behind one interface. |
| **Wiring** | [Dependency injection by type](REFERENCE.md#bootstrap), resolved and checked before anything serves. |
| **Lifecycle** | Many servers as [one app](REFERENCE.md#app), stopped gracefully together, with [zero-downtime Kubernetes rollouts](REFERENCE.md#zero-downtime-deployments-on-kubernetes) and no `preStop` hooks. |
| **Middleware** | [Recovery, logging, retries, distributed locks and guards](REFERENCE.md#middleware) on every server, and [CORS, rate limiting and JWT](REFERENCE.md#built-in-http-middlewares) on HTTP and gRPC. |
| **Messaging** | An [idempotent inbox](REFERENCE.md#consumer), a [transactional outbox](REFERENCE.md#producer-outbox-relay) and retry rules, on any broker. |
| **Observability** | [Structured logs](REFERENCE.md#logging) that trace every operation, with [secrets masked](REFERENCE.md#masking-sensitive-fields), and [metrics](REFERENCE.md#metrics) labelled alike on every server. |
| **Live configs** | [Typed feature flags and limits](REFERENCE.md#live-configs) that admins change while the app runs, version-checked. |
| **CLI** | [Commands](REFERENCE.md#cli) with the app's wiring, plus [SQL migrations](REFERENCE.md#migrate-sql-migrations) and [OpenAPI 3.1 generation](REFERENCE.md#openapi-api-documents). |
| **Testing** | [`forgotest`](REFERENCE.md#testing) boots the app `main` runs: a live server or the router in process, and one cron run or message delivery at a time. |

## Ready to use

forgo is the core. Two companions take you to production on day one:

| | |
| --- | --- |
| **[forgo-adapter](https://github.com/efegreat/forgo-adapter)** | Infrastructure you configure instead of write: PostgreSQL and MySQL, Redis, RabbitMQ, Google Cloud Pub/Sub and Redis Streams with dead-letter queues, SQL inbox, outbox and live config stores, Redis locks and rate limits, and OpenTelemetry metrics over OTLP or Prometheus. |
| **[forgo-scaffold](https://github.com/efegreat/forgo-scaffold)** | A complete backend to clone: an HTTP API, a worker and cron jobs on PostgreSQL, RabbitMQ and Redis, with metrics, live configs, migrations, Docker Compose, CI and tests. Run `docker compose up --build`, then replace the example domain with yours. |

The core never depends on them: each adapter implements a forgo interface that you can implement too.

## Getting started

```sh
go get github.com/efegreat/forgo          # the framework
go get github.com/efegreat/forgo-adapter  # the ready-made adapters, optional
```

forgo requires Go 1.27 or newer. Build your first service with the [Quick start](REFERENCE.md#quick-start), or start from the [scaffold](https://github.com/efegreat/forgo-scaffold).

## Documentation

- **[REFERENCE.md](REFERENCE.md)** is the complete guide: the bootstrap, the app lifecycle, every server, the CLI, testing, logging, metrics, live configs and [error codes](REFERENCE.md#error-code-reference).
- **[pkg.go.dev](https://pkg.go.dev/github.com/efegreat/forgo)** documents the API of every package, with runnable examples.
- **[PERFORMANCE.md](PERFORMANCE.md)** shows what an operation costs on each server, and how to measure it.
- **[forgo-adapter's REFERENCE.md](https://github.com/efegreat/forgo-adapter/blob/main/REFERENCE.md)** documents the configuration of every adapter.
- **[AGENTS.md](AGENTS.md)** holds the architecture and the rules, for contributors and coding agents working on forgo itself.

## Contributing

Bug reports and pull requests are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md). Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes.

## License

forgo is released under the [MIT License](LICENSE).
