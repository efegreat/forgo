# Performance

forgo does its work once, at startup, so that serving costs as little as it can. This page shows what that means in numbers, and how to measure it yourself. New here? Start with the [README](README.md).

## How

- **Resolved before serving.** Reflection, dependency lookup and the building of handler chains happen in `Bootstrap.Build`, `ServerFactory.New` and `IServer.Init`. The bootstrap is then sealed, so it never becomes a service locator at runtime.
- **Pre-built closures.** An operation runs a chain built once. A server allocates the operation, its `Args` and its transport state together, in one struct, and shares static args copy-on-write instead of copying them.
- **Logging without allocations.** Logs are written with [zerolog](https://github.com/rs/zerolog). The masking of sensitive fields is built before serving, scans a body once without allocating, and does not run for lines below the log level.
- **Planned conversions.** The gRPC server plans the conversions between proto messages and your structs once, from the descriptors, and converts generated messages through their Go structs. It logs them the same way, through a plan of each method's messages.
- **Guarded in CI.** Allocation tests pin the hot paths of request IDs, logging, masking and metrics, so an allocation added to one fails CI.

## What an operation costs

Apple M5, Go 1.27.0, darwin/arm64. Each figure is the median of three runs of `-benchtime=2s`. Servers log every line to `io.Discard`, so each one is encoded in full.

With the base chain, every operation gets a request ID, a logger carrying its trace fields, a log of its input and its result, an error mapped for the client, and panic recovery. [Middleware order](REFERENCE.md#order) lists each server's chain.

| Operation | Base chain | Without it | Benchmark |
| --- | ---: | ---: | --- |
| HTTP `GET`, headers logged and masked | 2.7 µs, 43 allocs | | `server/http` `BenchmarkServeHTTP/get` |
| HTTP `POST`, JSON body bound, validated and logged | 3.5 µs, 54 allocs | | `server/http` `BenchmarkServeHTTP/post` |
| gRPC unary call, business structs | 21.5 µs, 178 allocs | 17.0 µs, 156 allocs | `server/grpc` `BenchmarkUnary` |
| gRPC bidi stream, one message each way | 6.9 µs, 43 allocs | | `server/grpc` `BenchmarkBidiMessage` |
| Consumed message, JSON payload bound | 1.6 µs, 26 allocs | 1.0 µs, 23 allocs | `server/worker` `BenchmarkHandleMessage` |
| Cron run | 358 ns, 5 allocs | 23 ns, 2 allocs | `server/cron` `BenchmarkRun` |

The HTTP figures include building the request, with six typical headers, and the response recorder. The gRPC figures include the client and an in-memory connection. The worker's broker and inbox do nothing, so the figures are forgo's own work.

## Over grpc-go

The gRPC benchmarks also serve the same calls from a bare grpc-go server, with the generated code, as a baseline.

| Server | Unary call | Bidi message |
| --- | ---: | ---: |
| grpc-go | 16.4 µs | 6.8 µs |
| forgo, proto messages, no base chain | 16.6 µs (+1%) | |
| forgo, business structs, no base chain | 17.0 µs (+4%) | |
| forgo, business structs, base chain | 21.5 µs (+32%) | 6.9 µs (+3%) |
| forgo, base chain, every stream message logged | | 7.9 µs (+16%) |

Converting your structs costs 4% over the generated code. An observer that does nothing adds no measurable time (`BenchmarkUnary/base_observed`).

## Opt-in middlewares

Each middleware runs alone around a handler that does nothing (`server/http` `BenchmarkMiddlewares`). The table shows what it adds to the 74 ns and 7 allocations of the operation alone.

| Middleware | Adds |
| --- | ---: |
| CORS, simple request | 152 ns, 4 allocs |
| CORS, preflight | 389 ns, 8 allocs |
| Rate limit, in memory | 188 ns, 6 allocs |
| JWT, HS256 | 2.1 µs, 39 allocs |
| JWT, RS256 | 21.6 µs, 44 allocs |
| JWT, ES256 | 36.2 µs, 55 allocs |

The JWT figures are mostly the verification of the signature.

## Measure it yourself

```sh
go test -run '^$' -bench . -benchmem -benchtime=2s -count=3 ./server/... ./middleware
```

With `./...` in place of the packages, it also runs the benchmarks of every other hot path, such as the masking of logged JSON in `redact`. Compare two runs, such as before and after a change, with [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).
