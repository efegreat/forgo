# server/http: improvements

Findings from the pre-v1 review of `server/http/`.

## Must have

None.

## Should have

None.

## Could have

### `405 Method Not Allowed`

A known path with another method gets `404 nf`, because the `not_found` fallback claims every path before ServeMux can answer `405` with an `Allow` header.

### Hijacked connections are not waited for

`Shutdown` neither waits for hijacked connections, such as WebSockets, nor tells their handlers to stop, and `Config` does not expose `RegisterOnShutdown`. Their handlers can still run while the resources close. Listed in AGENTS.md "Known gaps".

### Handlers cannot read the raw body

The default binder decodes the body into every request type, `struct{}` included, whatever its `Content-Type`. A handler that needs the raw body, such as a webhook that verifies a signature over it or an upload, finds it consumed, or gets `json_type` for a body that is not JSON. bingo's `WithoutBodyDecoding` turns decoding off for the whole binder only. Skipping the body for request types without `json` fields, or a request type that asks for the raw body, would cover it.

### Reuse the default serializer

`newDefaultSerializer` is unexported, so a custom serializer cannot decorate the default one, and an application that rebuilds the base chain with `DisableBaseMiddlewares` cannot pass it to `ErrorResponder`. The default mappers are exported, so the default `ErrorMapper` can be rebuilt.

### Quieter logs for what clients cause

- `net/http`'s error log lines, mostly TLS handshake errors from scanners and clients that hang up, are logged as errors with moderate severity (`logging.go:256`). Warnings would keep them out of alerts.
- A response aborted with `http.ErrAbortHandler`, as `httputil.ReverseProxy` does when the client goes away, is logged as a critical failure with code `x`.

### Public messages of the default mappers

The `json_type` message of `MapBody` sends Go type names to clients (`required type: money.Amount`), and `MapValidation` names only the leaf field of a nested struct (`field: name`), not its path (`items[0].name`).

### Readable codes

`nf` for "not found" is hard to guess. Codes are part of the wire contract, so this is the last chance to rename it, such as to `not_found`.

### Smaller defaults and checks

- `ReadHeaderTimeout` falls back to `ReadTimeout` (60 seconds), so a client that sends headers slowly holds a connection for a minute. A 10-second default is common.
- A `TLSConfig` without certificates fails only in `Serve`.
- Route names are not checked. An empty name logs an empty `op_name`, and two routes with one name share it, and their OpenAPI `operationId`.

### Require a JSON content type for JSON bodies

bingo decodes the body whatever its `Content-Type`. A cross-site `fetch` with `Content-Type: text/plain` needs no CORS preflight, so with `JWTConfig.Cookie` and a `SameSite=None` cookie, another site can send authenticated JSON requests. Answering `415` to bodies that are not `application/json` would close it.

### Testing handlers that read JWT claims

`jwtClaimsKey` and the payload of `JWTClaims` are unexported, so a handler test has to sign a real token to get claims into its context. A `ContextWithJWTClaims(ctx, claims)` would help.

### `Forwarded`

`ClientIP` reads `X-Forwarded-For` only, not the standard `Forwarded` header (RFC 7239).

### The response recorder buffers bodies nobody logs

Every response keeps up to 32 KiB of its body for the `Recorder` (`server.go:160`), also with `DisableBaseMiddlewares`, for routes with `ArgKeyRecorderDisabled`, and when info is not logged. It costs one allocation and a copy per response. Moved from `internal/improvement.md`.

### `Server` is exported

As `server/cron/improvement.md` notes for every server, nothing needs `*http.Server`, since `Config.New` returns a `forgo.IServer`. Decide it for all servers at once.

## Performance

Measured on an Apple M5. `go test ./server/http -bench ServeHTTP` takes about 3 µs and 52 allocations per GET, and 3.8 µs and 63 allocations per POST, with the base chain and request logging. About 30 of the allocations belong to the server, the rest to building the test request and recorder. The base chain is about 560 ns of it (see `middleware/improvement.md`), and the body limit one allocation (64 bytes) on requests with a body. Among the opt-in middlewares, CORS costs about 240 ns, the rate limit 280 ns and JWT verification 2.2 µs with HS256, 21 µs with RS256 and 35 µs with ES256, almost all of it the signature check.

- `X-Request-ID` is not in canonical form (`X-Request-Id`), so reading it from the request and setting it on the response allocate twice per request. The rate limit headers are already declared canonical for this reason.
- `MemoryRateLimiter.Allow` takes about 30 ns, half of it reading the clock. It does not get faster with more cores in a parallel benchmark. Padding its shards to a cache line gains about 15%, which is nothing next to a request, so it is not worth the memory.
- Struct layout: only `corsPolicy` (120 bytes, 16 wasted in its size class) could shrink, and it is allocated once per middleware. The per-request `State` (16 bytes), response recorder (64) and default serializer (48) have no padding to remove.
