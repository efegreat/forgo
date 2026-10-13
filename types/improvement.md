# types: improvements

Findings from the pre-v1 review of `types/`.

## Must have

None.

## Should have

None.

## Could have

### `GetTyped` cannot tell a missing key from a mismatched value

It returns the zero value for a missing key, a value of another type, a failed conversion and a stored zero. A `(T, bool)` form would tell them apart. Adding one later, under another name, is not breaking.

### `GetTyped` converts `json.Number` to exact types only

`GetTyped[Port]` for `type Port int` returns zero for a `json.Number`. Converting by kind needs reflection. `GetTyped` is not on a hot path, but the gain is small.

### The package name

`types` is in the Go code review list of meaningless package names, and it reads like `go/types` in imports. A name for what it holds would be clearer. Renaming it breaks forgo, forgo-adapter, applications and docs, so it is only cheap before v1. This is the maintainer's decision.

### `Scan` of an empty value

`Scan("")` and `Scan([]byte{})` fail with `json map: decode: EOF`. A text column holding an empty string could be read as `NULL` instead. An error is defensible, since an empty string is not JSON.

### Compound operations

There is no `LoadOrStore`, `Swap` or `Update(key, func(old V, ok bool) V)`. Goroutines of one operation that share `Args` cannot check and set a key atomically. Nothing in the framework needs it yet.

## Performance

Measured with `go test ./types -bench . -count=3` on an Apple M5:

- `Get` costs about 1.7 ns on an empty map, which skips the lock, and 10.5 ns on a hit. Neither allocates.
- `NewMapCopyOnWrite` costs 7.4 ns and one 48-byte allocation, against 51 ns and 3 allocations (384 B) for `NewMapWithData`. Transports already use it for `Args`.
- `NewMap` and `NewMapWithData(nil)` allocate the Go map up front (2 allocations). `NewJSONMap` must keep doing so, since its map is what makes it store `{}` instead of `NULL`.
- For three headers, `Value` costs 310 ns (9 allocations) and `Scan` 575 ns (14 allocations). `Scan` needs a `json.Decoder` for `UseNumber` and for the trailing-data check. The outbox relay scans once per claimed row, and the database round trip dominates. Not worth a second code path.

Memory layout: `Map` is 48 bytes (map pointer 8, `RWMutex` 24, `bool` padded to 8, `atomic.Int64` 8), which is exactly a size class. Reordering the fields does not shrink it, and an `atomic.Bool` instead of the size counter would make it 40 bytes, which the allocator rounds up to 48 anyway. `JSONMap` is the same struct. Nothing to repack.
