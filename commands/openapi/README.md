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

With options:

```go
func NewOpenAPICommand() cli.CommandFactory {
    return func() cli.Command {
        return openapi.NewCommand(httpConf,
            openapi.WithInfo(openapi.Info{
                Title:       "Shop API",
                Description: "Public HTTP API of the shop",
                Version:     "1.4.0", // for the document of unversioned routes
            }),
            openapi.WithSchema[decimal.Decimal](openapi.Schema{"type": "string", "format": "decimal"}),
            openapi.WithDocument(addServers),
        )
    }
}

// {Name: "openapi", Command: cli.As(NewOpenAPICommand)}
```

| Option | Effect |
| --- | --- |
| `WithInfo(Info)` | Title (default `API Doc`), description (default `HTTP API Doc`) and the version of the unversioned document (default `1.0.0`). |
| `WithSchema[T](Schema)` | Documents `T` as the given schema everywhere: bodies, responses and parameters. See [Supported types](#supported-types). |
| `WithDocument(hook)` | Runs `hook(version, doc)` on every generated document, to add servers, security schemes, tags and so on. See [Customizing documents](#customizing-documents). |

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

Positional arguments are rejected: `app openapi docs/openapi.json` fails instead of
writing `./openapi.json`.

The command writes **one document per route version**, adding the version
before the extension:

| Route version | Output for `-o=./docs/openapi.json` | Example path |
| --- | --- | --- |
| `v1` | `./docs/openapi-v1.json` | `/v1/health` |
| `v2` | `./docs/openapi-v2.json` | `/v2/health` |
| none | `./docs/openapi.json` | `/health` |

- Each document contains only its version's routes and schemas. `info.version`
  is the route version, or `Info.Version` for unversioned routes.
- The parent directory must exist, and an existing output must be a regular
  file, not a symlink. Existing files keep their permissions.
- Every document is generated and encoded before any file is written, so a
  generation error leaves all files untouched. Each file is replaced
  atomically, but not together with the others: if writing one fails, the files
  written before it stay replaced.
- Files of removed versions are not deleted.

Routes with `DisableAPIDoc` are left out, such as private admin routes, and a
version whose routes all have it gets no document. The server serves them as
usual.

## Routes

Paths use `ServeMux` syntax, as the server does. A path the server cannot
register, such as `/users/{id:[0-9]+}` or `/files/{name}.json`, fails generation
with ServeMux's own error. OpenAPI has no remainder wildcard and no
end-of-path anchor, so:

| Route path | Documented as |
| --- | --- |
| `/users/{id}` | `/users/{id}` |
| `/files/{path...}` | `/files/{path}`, described as matching the rest of the path |
| `/{$}`, `/users/{$}` | `/`, `/users/` |

The method must be one OpenAPI can describe (`GET`, `PUT`, `POST`, `DELETE`,
`OPTIONS`, `HEAD`, `PATCH` or `TRACE`), in upper case: ServeMux methods are
case-sensitive, so a route with `"get"` never matches a `GET` request. A route
with an empty method matches every method, which one operation cannot
describe: give it a method, or `DisableAPIDoc`.

Errors name the route by its method and path, and by its `Name` when set:
`openapi: route "getUser" (GET /v1/users/{id}): …`.

## Requests

Request fields are documented from the sources they are bound from:

| Tag | Documented as |
| --- | --- |
| `uri:"id"` | Required path parameter |
| `header:"X-Request-ID"` | Header parameter |
| `query:"currency"` | Query parameter |
| `json:"name"` | Request-body property |

- `"-"` disables a source: `uri:"-" query:"id"` is a query parameter.
- Fields without a source tag are left out.
- A field with several sources, such as `query:"currency" json:"currency"`, is
  documented in each, with a description naming the others and the one that
  wins when a request sends several (header, then path, then query, then body,
  as the binder applies them). Since any of them satisfies `required`, none is
  documented as required, except a path parameter, which always is.
- Every `uri` field must match a path wildcard, and every wildcard must be
  bound by a `uri` field of the request or of a middleware's `Params`.
- Parameters are never `null`: a pointer only makes a parameter optional.
- Query arrays use repeated keys (`style: form, explode: true`). Header arrays
  take one header line per value, as the binder reads them, and say so in their
  description. A `uri` field cannot be a slice or an array, because a wildcard
  binds one value.
- A parameter's type is a primitive, `time.Time`, an
  `encoding.TextUnmarshaler`, a type with `WithSchema`, or a slice or array of
  these. Other types fail generation.

`validate` tags become schema constraints: `required`, `min`, `max`, `len`,
`gt`, `gte`, `lt`, `lte`, `oneof`, `email`, `uuid*`, `hostname`,
`hostname_rfc1123`, `url`, `uri`, `ip`, `ipv4` and `ipv6`. Other rules are
ignored, and rules after `dive` or `keys` are not applied to the collection
itself.

### Middleware parameters

Server and route middlewares attached with `http.MiddlewareAs[Params]`, or
with `http.WithContract` and a `Params` type, add the fields of `Params` to the
parameters of every route they run on:

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
- To read middleware contracts, the generator calls each middleware factory
  with a resolver that provides nothing of the application.
  `http.MiddlewareAs` and `http.WithContract` factories report their contracts
  without calling their constructor or factory, and `http.RateLimit`,
  `http.RateLimitWith` and `http.JWTAuth` report theirs without a limiter.
  Other factories are ignored: they fail to resolve their dependencies, or
  construct a middleware that is discarded.

Every operation also gets the optional `X-Request-ID` header parameter, which
the server takes the operation ID from.

### Authentication, rejections and headers

Middleware contracts also document how a middleware authenticates requests,
the responses it answers itself and the headers it sets. `http.JWTAuth` and
`http.RateLimit`/`RateLimitWith` declare theirs, and `http.WithContract`
declares one for a middleware of your own:

```go
var APIKeyAuth = http.WithContract(http.MiddlewareContract{
    Auth: &http.AuthContract{Credentials: []http.Credential{
        {Kind: http.CredentialHeader, Name: "X-API-Key"},
    }},
    Rejections: []http.Rejection{{Err: ErrUnknownKey}},
}, forgo.MiddlewareAs(NewAPIKeyAuth))
```

- Routes list the credentials of their middlewares in their `security`, and
  no credential when `Optional` is set. The schemes are added to
  `components.securitySchemes`. A bearer token is the `bearerAuth` scheme,
  basic authentication `basicAuth`, and a key in a header, query parameter or
  cookie an `apiKey` scheme named after it. `http.JWTAuth` accepts a bearer
  token (`bearerFormat: JWT`) and its `Cookie` when one is set. A route behind
  several authenticating middlewares needs a credential for each.
- Each rejection is documented with the body and status your serializer
  writes for its error, such as the `401` of `http.JWTAuth` with
  `WWW-Authenticate` and the `429` of `http.RateLimit` with `Retry-After`.
- A response carries the headers of the middlewares that ran before it. A
  middleware's own headers, such as the `RateLimit-*` headers unless
  `DisableHeaders` is set, are on every response of the routes it runs on.
- The contract only documents, and the server never checks it. Contracts that
  cannot be documented, such as a credential without a name, and two
  different schemes with one name fail generation.

A `WithDocument` hook only adds the authentication no contract can declare,
such as OAuth 2 scopes. See [Middleware contracts](../../REFERENCE.md#middleware-contracts)
for every field.

## Responses

The generator probes the route's serializer to find where the result lands in
its envelope, then places the reflected response type there. Status, media
type and response headers come from the probe. Probing the serializer's
`Error` produces the `default` error response.

`http.EmptyHandler` and `http.HealthHandler` routes, and handlers with an
empty contract, have no request, other than `X-Request-ID`, and no result. `http.LiveConfigsHandler`
routes have a result whose config values have no schema, and
`http.SetLiveConfigHandler` routes have their `name` and `If-Match` parameters
but no body, because it depends on the config.

## Supported types

Primitives, pointers (nullable), slices, arrays, maps with JSON-compatible
keys, nested and recursive structs (as `components/schemas`), embedded structs
with JSON dominance rules, `time.Time`, `json.Number`, `json.RawMessage`,
`types.JSONMap[V]` (an object of `V` values), and text types:
`encoding.TextUnmarshaler` in requests, which are decoded, and
`encoding.TextMarshaler` in responses, which are encoded.

A type with its own JSON encoding (`json.Unmarshaler` in a request,
`json.Marshaler` in a response) fails generation, because its shape cannot be
inferred, and so do channels and functions. Describe such types with
`WithSchema`, which also gives named types a format or enum values:

```go
openapi.WithSchema[decimal.Decimal](openapi.Schema{"type": "string", "format": "decimal"})
openapi.WithSchema[OrderStatus](openapi.Schema{"type": "string", "enum": []string{"open", "paid"}})
```

A field's `validate` tag still adds its constraints, and `*T` stays nullable in
bodies unless it has a `WithSchema` of its own.

### Component names

Named struct types become components named after the type: `Product`, and
`PageProduct` for `Page[shop.Product]`. A type used in both a request body and
a response has two schemas, because request schemas keep only `json` fields and
carry validation: the request one is named `ProductRequest`. Names that collide
get the package name as a prefix (`ShopProduct`), then a hash suffix
(`Product_1a2b3c4d5e6f`) if that is not enough.

## Customizing documents

Routes do not declare servers or tags, nor authentication that no middleware
contract declares. Add them with a `WithDocument` hook, which runs on every
document after generation and gets the route version (empty for unversioned
routes). `Document.Operation` finds an operation by its `operationId`, the
route `Name`:

```go
func addServers(version string, doc *openapi.Document) error {
    doc.Servers = []openapi.Server{{URL: "https://api.example.com"}}
    if health := doc.Operation("health"); health != nil {
        health.Tags = []string{"ops"}
        doc.Tags = []openapi.Tag{{Name: "ops", Description: "Operations."}}
    }
    return nil
}
```

Document an API key middleware of your own with
[`http.WithContract`](#authentication-rejections-and-headers) instead. A hook
is for what a contract cannot declare, such as OAuth 2 scopes: add the scheme
to `doc.Components.SecuritySchemes` and its requirement to `doc.Security`, or
to the `Security` of the operations it guards.

An error from a hook fails generation.

## Programmatic use

```go
gen := openapi.New(httpConf, openapi.WithInfo(openapi.Info{Title: "Shop API"}))
docs, err := gen.GenerateAll(ctx) // map[version]*openapi.Document
if err != nil {
    return err
}
return openapi.WriteFiles(ctx, "./docs/openapi.json", "json", docs)
```

- `New` sanitizes the config, so the server's default serializer is documented
  unless the config sets its own.
- `Generate` returns the document of a config whose documented routes share one
  version.
- `openapi.Marshal(doc, "yaml")` and `openapi.WriteFile` work on a single
  document. `openapi.VersionOutputPath` returns the file `WriteFiles` writes a
  version to.

## Testing

```sh
go test -race ./commands/openapi
```

`TestGolden` compares generated documents with `testdata/golden*.json` and
`testdata/golden*.yaml`. After an intended change in the output, rewrite them
with `go test ./commands/openapi -run TestGolden -update` and review the diff.
