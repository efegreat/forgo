# openapi: improvements

Findings from the pre-v1 review of `commands/openapi/`.

## Must have

None.

## Should have

None.

## Could have

### Command conveniences

- A `-check` flag for CI that fails when the files on disk differ from the generated ones. This is the usual guard for "docs from code".
- Print the written paths. Today the command is silent apart from the operation logs.
- Accept `-format=yml`, since `.yml` is accepted as an extension.
- `-o=-` for stdout, when there is a single document.

### Every command bootstraps every resource

`openapi` needs no database, but in CI it fails when a resource pings on startup. This is a CLI concern, already in `cli/improvement.md`. Mention the workaround (a second `cli.New` without resources) in this package's README.

### Route-level documentation

`WithDocument` hooks can set `Summary`, `Tags`, `Deprecated` and `Security` per operation, but only by route name, away from the route. Consider `RouteConfig.Summary`/`Tags`/`Deprecated` (`generator.go:246`). Errors are documented as one `default` response. Handlers could declare the `operation.Error` codes and statuses they return, so 400, 404, 409 and so on are documented with their codes.

### More validation coverage

- Apply the rules after `dive` to `items`, and the rules after `keys` to `propertyNames`, instead of dropping them (`validation.go:40`).
- `required` means non-zero in validator: `minLength: 1` for strings, `const: true` for booleans.
- `,string` fields lose their constraints: `object` replaces the schema after `validation` built it (`schema.go:384`). Apply the constraints to the string form, or keep them in `allOf`.
- `json.Number` is a string kind to validator, so `min`/`max` limit its length, but `validation` documents them as `minimum`/`maximum` (`validation.go:18`).
- `alpha`, `alphanum`, `numeric`, `startswith`, `endswith`, `contains`, `e164`, `datetime` map to `pattern` or `format`.

### Catch binding mistakes that bingo only reports at the first request

bingo rejects textual tags behind a pointer or inside a collection, and on unexported fields, when it first binds the type. That happens on the first request, not at startup. The generator walks pointers to structs (`request.go:186`) and skips unexported fields (`request.go:158`), so it documents these as if they worked. Rejecting them would make `openapi` an early check for these bugs.

### Version normalization

Versions are grouped by their raw string (`generator.go:96`). `Version: "v1"` and `Version: "/v1/"` are served under the same prefix (`routePath` trims slashes), but they produce two documents, `openapi-v1.json` and `openapi-%2Fv1%2F.json`. Group by the trimmed version.

### Smaller code and style points

- `serializer.go:10` imports `net/http/httptest` into production code, so every application binary with the `openapi` command links it. A ten-line `http.ResponseWriter` recorder is enough.
- `WriteFiles` encodes every document twice (`writer.go:79`, then inside `WriteFile`). Encode once and write the bytes.
- The two built-in commands are registered differently: `migration.Factory(migration.CommandConfig{})` against `cli.As(openapi.NewDefaultOpenAPICommand(config.HTTP))`, which also stutters. An `openapi.Factory(conf, options...) cli.Factory` would match the migrate command and make `NewDefaultOpenAPICommand` unnecessary.
- `sameProbeError` only wraps `reflect.DeepEqual` (`serializer.go:56`). Inline it.
- `goto done` (`validation.go:41`) reads better as a labeled `break`. Mixed `sort` and `slices` usage: prefer `slices.Sorted(maps.Keys(...))` and `slices.SortFunc`. `fmt.Errorf` calls without arguments can be `errors.New` or sentinel errors (`ErrNoRoutes`). Group `markerKey` and `probeID` (`serializer.go:19`).
- `GenerateAll` says `no routes configured` when routes exist but all have `DisableAPIDoc` (`generator.go:99`).
- The `equivalent templated paths` error (`generator.go:170`) could say to use the same wildcard name, as OpenAPI requires.
- The older tests are still all in `generator_test.go`. The new ones are split by source file (`request_test.go`, `schema_test.go`, …). Move the rest.
- `Info` could carry the remaining OpenAPI info fields (`summary`, `contact`, `license`, `termsOfService`).
- The YAML writer quotes every key and string (`writer.go`). That is valid and safe, but hard to read. Plain scalars for safe strings would make diffs friendlier.

### Memory layout and performance

No changes recommended. Generation runs once, offline, and nothing is on a request path. `Parameter` has 7 bytes of padding, but reordering it would change the JSON key order of every generated document (`name`, `in`, `description`, `required`, …). The other padding (`RequestBody`, `schemaKey`, `jsonField`) is a single trailing `bool` group and cannot be removed. Probing the serializer twice per route, registering each path on a throwaway `ServeMux`, and building one private bootstrap per middleware per route are negligible.
