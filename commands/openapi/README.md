# forgo openapi

Package `github.com/efegreat/forgo/commands/openapi` generates OpenAPI 3.1 documents from a
`github.com/efegreat/forgo/server/http` configuration and provides the `openapi` CLI command. It
reads routes and types only: it never starts a server, resolves dependencies or
calls handlers. See also [REFERENCE.md](../../REFERENCE.md#openapi-api-documents).

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
- Path wildcards must match `uri` fields exactly.
- Query arrays use repeated keys (`style: form, explode: true`). Path and
  header arrays are comma-separated.
- Object-valued parameters are not supported and fail generation.

`validate` tags become schema constraints: `required`, `min`, `max`, `len`,
`gt`, `gte`, `lt`, `lte`, `oneof`, `email`, `uuid*`, `hostname`,
`hostname_rfc1123`, `url`, `uri`, `ip`, `ipv4` and `ipv6`. Other rules are
ignored, and rules after `dive` or `keys` are not applied to the collection
itself.

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
