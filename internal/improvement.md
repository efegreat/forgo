# internal: improvements

Findings from the pre-v1 review of `internal/headerlog` and `internal/redact`.

## Must have

None.

## Should have

None.

## Could have

### The cut header path allocates

When headers pass the limit, `headerlog.Log` logs them with `event.Str(key, string(buf[:limit]))`, which copies up to 32 KiB (`internal/headerlog/headerlog.go`). `event.Bytes(key, buf[:limit])` writes the same string without the copy, as `redact.Fields.LogJSON` already does. Separately, `Append` checks the limit only between headers, so a single huge header value, as message headers can hold, is encoded whole before the cut. Its buffer then grows past `maxPooledBuffer` and does not go back to the pool.

### One buffer pool

`headerlog` and `redact` each declare the same `bufferPool` and `maxPooledBuffer`. A small shared helper, such as `internal/bufpool`, removes the duplication, and both recorders then reuse the same warm buffers.

### `headerlog.Append` does not need to be exported

Only `Log` and the package's tests call it. Rename it to `appendHeaders`. This is an internal package, so nothing outside the module changes.

### Long escaped keys allocate

`hasKey` unescapes a key into a 64-byte stack buffer, with no bound (`internal/redact/json.go`). An escaped key longer than 64 bytes allocates as much as its length, and a hostile body can repeat that for every key. Folding shrinks a rune at most 3 times (the Kelvin sign is 3 bytes and folds to `K`), so `unescape` can stop once its output passes `3*maxLen` and report no match.

### Form bodies are dropped or logged in clear text

`application/x-www-form-urlencoded` is the usual body of login and OAuth token endpoints (`grant_type=password&password=...`), which is exactly where secrets travel. Without `SensitiveFields` such a body is logged whole. With `SensitiveFields` it is logged as `""`. `server/http`'s `maskQuery` already masks this format. Move it to `redact` and have the HTTP request recorder use it when the `Content-Type` is a form.

### Invalid UTF-8 inside JSON strings reaches the log raw

`jsonMasker` accepts invalid UTF-8 inside strings, as `json.Valid` does, and `RawJSON` writes it unchanged. The log line is then not valid UTF-8, which RFC 8259 requires and strict consumers reject, while other zerolog fields write U+FFFD instead. The masker already copies when it drops whitespace, so it can replace invalid bytes in strings at almost no cost.

### Document what masking does not reach

Masking does not reach JSON that is encoded inside a string, such as `{"payload":"{\"password\":\"x\"}"}`. Message envelopes and webhooks often look like this. Add one sentence to "Masking sensitive fields" in REFERENCE.md.

### HTML escaping in header logs

`AppendString` escapes `<`, `>` and `&` to stay byte-identical to `encoding/json`, so a `Link` header is logged as `<https://...>; rel=\"next\"`. zerolog escapes HTML in no other field, and before v1 nothing depends on these exact bytes. Without HTML escaping, header logs are easier to read. The tests would then compare against an `encoding/json` encoder created with `SetEscapeHTML(false)`.
