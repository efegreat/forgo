// Package types holds the concurrency-safe maps the framework passes between its parts.
//
// Map is the generic map behind operation.Context.Args. Its zero value is ready to use, lookups in an
// empty Map take no lock, and NewMapCopyOnWrite lets every operation read a route's static args in
// place until it writes to them.
//
// JSONMap is a Map with string keys that encodes as a JSON object, both through encoding/json and as
// a database value, so it can be stored in a JSON column as is. worker.Message.Headers is one.
//
// See [types.Map and types.JSONMap] in REFERENCE.md.
//
// [types.Map and types.JSONMap]: https://github.com/efegreat/forgo/blob/main/REFERENCE.md#typesmap-and-typesjsonmap
package types
