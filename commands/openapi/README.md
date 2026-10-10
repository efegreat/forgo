# forgo openapi

Package `github.com/efegreat/forgo/commands/openapi` generates OpenAPI 3.1 documents from a
`github.com/efegreat/forgo/server/http` configuration and provides the `openapi` CLI command. It
reads routes and types only: it never starts a server, resolves the application's
dependencies or calls handlers. See also [REFERENCE.md](../../REFERENCE.md#openapi-api-documents).

## Setup

Pass the same `http.Config` value the HTTP server uses:

```go
{Name: "openapi", Command: cli.As(openapi.NewDefaultOpenAPICommand(httpConf))}
```

With your own title and description:

```go
func NewOpenAPICommand() cli.CommandFactory {
    return func() cli.Command {
        return openapi.NewCommand(httpConf, openapi.WithInfo(openapi.Info{
            Title:       "Shop API",
            Description: "Public HTTP API of the shop",
        }))
    }
}

// {Name: "openapi", Command: cli.As(NewOpenAPICommand)}
```

## Usage

```sh
app openapi -o=./docs/openapi.json
app openapi -o=./docs/openapi.yaml
app openapi -output=./docs/api.yml -format=yaml
```

| Flag | Meaning |
| --- | --- |
| `-o`, `-output` | Output filename base. Default `openapi.json`. |
| `-format` | `json` or `yaml`. Defaults to the output extension (`.yaml`/`.yml` → YAML, otherwise JSON). |

The command writes **one document per route version**, adding the version
before the extension:

| Route version | Output for `-o=./docs/openapi.json` | Example path |
| --- | --- | --- |
| `v1` | `./docs/openapi-v1.json` | `/v1/health` |
| `v2` | `./docs/openapi-v2.json` | `/v2/health` |
| none | `./docs/openapi.json` | `/health` |

- Each document contains only its version's routes and schemas, and
  `info.version` is the route version.
- The parent directory must exist. Each file is replaced atomically, and a
  failed run leaves existing files untouched.
- Files of removed versions are not deleted.

## Requests

Each request field is documented from one source, chosen by the precedence
`uri > header > query > json`:

| Tag | Documented as |
| --- | --- |
| `uri:"id"` | Required path parameter |
| `header:"X-Request-ID"` | Header parameter |
| `query:"currency"` | Query parameter |
| `json:"name"` | Request-body property |

- `"-"` disables a source: `uri:"-" query:"id"` is a query parameter.
- Fields without a source tag are left out.
- Every `uri` field must match a path wildcard, and every wildcard must be
  bound by a `uri` field of the request or of a middleware's `Params`.
- Query arrays use repeated keys (`style: form, explode: true`). Path and
  header arrays are comma-separated.
- Object-valued parameters are not supported and fail generation.

`validate` tags become schema constraints: `required`, `min`, `max`, `len`,
`gt`, `gte`, `lt`, `lte`, `oneof`, `email`, `uuid*`, `hostname`,
`hostname_rfc1123`, `url`, `uri`, `ip`, `ipv4` and `ipv6`. Other rules are
ignored, and rules after `dive` or `keys` are not applied to the collection
itself.

### Middleware parameters

Server and route middlewares attached with `http.MiddlewareAs[Params]` add the
fields of `Params` to the parameters of every route they run on:

```go
type TenantParams struct {
    TenantID string `header:"X-Tenant-ID" validate:"required,uuid"`
}

var TenantMiddleware = http.MiddlewareAs[TenantParams](NewTenantMiddleware)
```

- `Params` only documents. The server never binds or validates it, so keep it
  in line with what the middleware reads.
- `Params` takes `header`, `query` and `uri` fields only. A `json` field fails
  generation, because the body belongs to the handler.
- A `uri` field must exist in the path of every route the middleware runs on.
- A parameter declared by several middlewares or by the handler is documented
  once, as required when any declaration requires it. Declarations with
  different schemas fail generation.
- To read `Params`, the generator calls each middleware factory with a
  resolver that provides nothing of the application. `http.MiddlewareAs`
  factories report their `Params` without calling their constructor. Other
  factories are ignored: they fail to resolve their dependencies, or construct
  a middleware that is discarded.

## Responses

The generator probes the route's serializer to find where the result lands in
its envelope, then places the reflected response type there. Status, media
type and response headers come from the probe. Probing the serializer's
`Error` produces the `default` error response.

`http.EmptyHandler` and `http.HealthHandler` routes, and handlers with an
empty contract, have no request and no result.

## Supported types

Primitives, pointers (nullable), slices, arrays, maps with JSON-compatible
keys, nested and recursive structs (as `components/schemas`), embedded structs
with JSON dominance rules, `time.Time`, `json.Number`, `json.RawMessage` and
`encoding.TextMarshaler`.

Types with a custom `json.Marshaler`, channels and functions fail generation,
because their JSON shape cannot be inferred.

## Programmatic use

```go
gen := openapi.New(httpConf.Sanitize(), openapi.WithInfo(openapi.Info{Title: "Shop API"}))
docs, err := gen.GenerateAll(ctx) // map[version]*openapi.Document
if err != nil {
    return err
}
return openapi.WriteFiles(ctx, "./docs/openapi.json", "json", docs)
```

- Pass a sanitized config so the default serializer is used. `NewCommand` does
  this for you.
- A title is required.
- `openapi.Marshal(doc, "yaml")` and `openapi.WriteFile` work on a single
  document.

## Testing

```sh
go test -race ./commands/openapi
```
