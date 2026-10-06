# ForGo OpenAPI generator — route versions

Copy `openapi/` into your ForGo module. Imports use your existing `forgo`,
`forgo/http`, `forgo/operation`, and `forgo/transport` packages. No new external
dependencies are needed. Requires Go 1.22+ (`reflect.TypeFor`).

The generator consumes `http.Config.Routes` and `http.Config.Serializer`.
It reads `route.Handler.Contract()`, route names, descriptions, methods, and
paths. It does not construct an HTTP server, resolve dependencies, build
handlers, invoke application code, execute middleware, or create a binder.
It invokes the serializer factory and its Success/Error methods against fresh
`httptest.ResponseRecorder` instances.

## Command constructor

```go
import (
    "forgo"
    "forgo/openapi"
)

// Register this constructor through your existing CLI factory/adapter.
// httpConf is the same http.Config value used to register the HTTP server.
func NewDefaultOpenAPICommand() forgo.Command {
    return openapi.NewCommand(httpConf, openapi.WithInfo(openapi.Info{
        Title:       "API Doc",
        Description: "HTTP API Doc",
    }))
}
```

Register under the name `openapi` using your existing command registration
layer (for example, the existing `cli.As(NewDefaultOpenAPICommand)` adapter).
The command implements only:

```go
Init(*flag.FlagSet)
Validate() error
Run(context.Context) error
```

Invocation:

```sh
openapi -o=./docs/openapi.json
openapi -o=./docs/openapi.yaml
openapi -output=./docs/openapi.yml -format=yaml
```

`-o`/`-output` specifies a filename base. The command writes one document per
exact `RouteConfig.Version`, adding that version before the extension:

| Route version | Output for `-o=./docs/openapi.json` | Example path |
| --- | --- | --- |
| `v1.0` | `./docs/openapi-v1.0.json` | `/v1.0/health` |
| `v2.0` | `./docs/openapi-v2.0.json` | `/v2.0/health` |
| Empty string | `./docs/openapi.json` | `/health` |

Each document contains only its version's routes and component schemas.
`info.version` equals the exact route version (including the empty string for
unversioned routes). `openapi.Info` contains only `Title` and `Description`;
`Document.Info` also contains the derived `Version`. Route descriptions and
empty-handler behavior are preserved. Route names may repeat across separate
version documents but must be unique within a document.

All documents are generated before the command writes any files. Each file
replacement is atomic; the whole batch is not a filesystem transaction.
Version bytes unsuitable for filenames are percent-encoded, so unusual version
strings cannot introduce output subdirectories. The route prefix uses the
original version string. Stale files from removed versions are not deleted.

`-format` defaults to the output extension (`.yaml`/`.yml` -> YAML, otherwise
JSON). Create the parent directory first. Output uses a temporary sibling file
and an atomic rename; generation failures leave the previous file untouched.
Output is deterministic for a deterministic serializer.

## Programmatic generation

```go
generator := openapi.New(httpConf, openapi.WithInfo(openapi.Info{
    Title: "API Doc", Description: "HTTP API Doc",
}))
docs, err := generator.GenerateAll(ctx)
if err != nil {
    return err
}
return openapi.WriteFiles(ctx, "./docs/openapi.json", "json", docs)
```

`GenerateAll` returns `map[string]*Document`, keyed by exact route version.
`Generate` remains available for a config containing one route version; it
rejects mixed-version configs and directs the caller to `GenerateAll`.
`VersionOutputPath(base, version)` returns a document's output path.
You can also call `openapi.Marshal(doc, "json")` or `"yaml"` for an individual
document, or `WriteFile` to explicitly choose its output filename.
The output is OpenAPI **3.1.0**, usable by OpenAPI-aware Swagger tools.

## Request conventions

The generator is independent of the active binder. It selects one enabled
source per field using `uri > header > query > json`:

| Tag | Documented location |
| --- | --- |
| `uri:"id"` | Required path parameter |
| `header:"X-Request-ID"` | Header parameter |
| `query:"currency"` | Query parameter |
| `json:"name"` | JSON request-body property |

`"-"` disables that particular source. For example, `uri:"-" query:"id"`
selects query. Empty textual tag names fail generation. `json:",omitempty"`
uses the Go field name. Fields without an enabled source tag are excluded
from the request body. Unnamed embedded structs are traversed for tagged
fields; nested structs are traversed for textual parameters. A named JSON
object requires a JSON tag on its containing field.

A selected textual source is excluded from the JSON schema even when the
field also has a JSON tag. This describes one client-facing convention,
rather than every binding option offered by a particular binder.

Header names are canonicalized. Duplicate names in the same source fail.
Path placeholders must have exactly matching `uri` fields. Mux-style paths
such as `/products/{id:[0-9]{2,}}` become `/products/{id}`; router regular
expressions are not translated into schema validation. Equivalent paths
using different placeholder names, duplicate operations, duplicate route
names, and unsupported HTTP methods fail generation.

Query arrays use `style: form, explode: true` (repeated query keys).
Path/header arrays use OpenAPI's default simple serialization (comma-separated
values). A custom binder using another encoding may differ from these
conventions. Object-valued textual parameters and nested textual collections
are unsupported; generation reports an error.

## Validation and schemas

Supported validation hints: `required`, `min`, `max`, `len`, `gt`, `gte`, `lt`,
`lte`, `oneof`, `email`, `uuid` variants, `hostname`, `hostname_rfc1123`, `url`,
`uri`, `ip`, `ipv4`, and `ipv6`. Numeric constraints retain integer precision,
including the full uint64 range. String and collection limits use length/item/
property constraints. Unknown rules, OR rules, and conditional/cross-field
rules are ignored. Parsing stops at `dive`/`keys`: element rules are not
incorrectly applied to the containing collection. Nested collection element
validation is outside v1 scope. The generator does not execute validators.

Validation is best effort: `required` marks field presence, but does not fully
model Go zero-value validation. `omitempty` skips before a subsequent required
rule; optional zero-value bypasses are not otherwise modeled. `json:",string"`
uses a string wire schema and does not preserve numeric validation constraints.
Slice-length constraints on base64 JSON byte slices are omitted because Go
byte length differs from encoded string length.

Supports primitives, pointers, slices, arrays, JSON-compatible map keys,
nested/recursive structs, `time.Time`, `json.Number`, `json.RawMessage`,
and text marshalers. JSON byte slices use base64 strings. Pointers, slices,
and maps can describe JSON null. Struct components are deduplicated separately
for request and response modes, with deterministic names including a type hash.
Response models follow ordinary exported-field/JSON-tag rules, including
untagged fields, embedded-field dominance, and omitted fields. `omitempty`
changes property presence, without removing that property from the schema.

Arbitrary `json.Marshaler` types cannot be inferred and fail explicitly,
except the known standard-library types above. Channels, functions, and other
unsupported JSON types also fail. Interface/RawMessage fields intentionally
have unconstrained schemas.

## Serializer inspection and empty handlers

Typed routes use a unique JSON marker to find where the serializer places the
response. That location is replaced by the reflected response model; the
marker does not appear in the output. Markers can be nested, repeated, or at
the JSON root. Status, media type (including vendor `+json` types), and response
header names come from the probe. Charset parameters are stripped from the
OpenAPI media type.

An empty `HandlerContract{}` means **no request and nil success result**.
The generator calls `Success(nil)`, matching the intended EmptyHandler
behavior and omitting `result` with the supplied default serializer.
`Success(struct{}{})` instead produces `"result": {}` with that serializer:
an `any` containing an empty struct is non-nil. Ensure `newEmptyHandler`
calls `Success(nil)` if omission is the intended runtime contract.

The error probe is `operation.Error{Status: operation.StatusInvalidArgument,
Code: ..., Message: ...}`. Its observed response becomes `responses.default`.
Returning the original operation error after writing, as your supplied default
serializer does, is accepted; serialization/write failures are propagated.
One probe provides a general error scaffold, not an exhaustive description
of all errors or status-dependent serializer branches.

Envelope fields are inferred from the observed sample; omitted envelope fields
cannot be discovered and are not declared required. Response-model fields are
always reflected independently. HEAD/204/205 responses have no documented body.
A serializer that rejects, transforms, or discards the marker fails generation
unless the response has no HTTP body. Type-dependent custom serializers need a
future explicit schema hook. Text error responses are supported; non-JSON
typed success formats are outside v1 scope.

`Config.Serializer` must be explicit. Your custom `NewSerializer` already meets
this requirement. If using ForGo's unexported default serializer, expose this
small general-purpose wrapper **in the existing `http` package**:

```go
func DefaultSerializer(id string, w http.ResponseWriter) transport.ISerializer {
    return newDefaultSerializer(id, w)
}
```

Then use `Serializer: http.DefaultSerializer` in the HTTP config. This exports
the existing factory without introducing OpenAPI metadata into HTTP config.
The generator does not duplicate or guess the default serializer implementation.

## Verification

The included Go tests cover source precedence, empty handlers, optional response
fields, custom nested envelopes, vendor media types, recursion, JSON embedding,
path mismatches, probe failures, validation precision, command flags, stable
output, and preserving an existing file after failed generation.

Verified with Go 1.25.1 using minimal compilation fixtures matching the supplied
ForGo API signatures; the full ForGo repository was not available in this
workspace. Run `go test -race ./openapi` and `go vet ./openapi` after copying
into your repository to verify integration against the real framework types.
The package imports assume `HandlerFactory.Contract()` and
`operation.StatusInvalidArgument` are exported with the names discussed.

`examples/openapi-v1.0.json` and `.yaml` show a product endpoint and an empty
health endpoint with `/v1.0` prefixes. `openapi-v2.0.*` contains only its health
endpoint. `openapi.*` contains an unversioned readiness endpoint. Each JSON/YAML
pair describes the same document. `examples/default_command.go.txt` contains the
requested constructor to place in your application package where `httpConf`
is defined.
