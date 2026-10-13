# server/worker: improvements

Findings from the pre-v1 review of `server/worker/`.

## Must have

None.

## Should have

None.

## Could have

### `ConsumerServer` and `ProducerServer` are exported

As `server/cron/improvement.md` notes for every server, nothing needs them, since `New` returns a `forgo.IServer`. Decide it for all servers at once.

### Unknown messages on shared queues

A message whose name has no handler is rejected and logged as an error (`nf_mh`, moderate severity). A subscriber that reads a queue or topic shared with other consumers would rather acknowledge such messages and skip them. A per-subscriber choice between rejecting and acknowledging would cover both. `nf_mh` is also the only worker code not named `msg_*`, such as `msg_unknown`.

### Messages without a stable ID

A message that reaches the consumer without an ID header, such as one published to RabbitMQ by another producer without `message_id`, gets a fresh ID on every delivery. Its failures are retried until the broker's own delivery limit, and its inbox rows pile up. The consumer could reject such messages instead of handling them, once `NewMessageFromDelivery` reports that the ID is missing.

### `msg_elapsed` for messages without a timestamp

`NewMessageFromDelivery` sets `CreatedAt` to the Unix epoch when the timestamp header is missing, so `msg_elapsed` logs about 56 years. A zero `CreatedAt`, with `msg_elapsed` left out, would be clearer.

### The error percentage hook

`OnErrorPercentage` receives neither the percentage nor the counts of the batch, and it runs on the relay's goroutine, so a slow hook delays the relay. `ErrorPercentage` above 100 is accepted and never triggers. A batch hook that receives the counts would also serve metrics.

### Errors logged twice

A subscriber that fails is logged as `subscriber error`, then again by `App` as `serve error`. An error that `Recorder` logs as `operation failed` is logged again as `message left unsettled, retrying` when the chain leaves the delivery unsettled.

### `Init` leaks on failure

When creating the second subscriber or publisher fails in `Init`, the ones already created are not closed. Closing the broker resource closes them later.

### `MaxAttempt` per handler

`MaxAttempt` is per subscriber. Handlers with different costs of failure, such as a payment and a notification, cannot have different limits without separate subscribers.

## Performance

Measured with `go test ./server/worker -bench HandleMessage` on an Apple M5, with fakes for the broker and the inbox. A message handled successfully costs about 1.9 µs and 30 allocations (2.9 KB) with the base chain, and 1 µs and 25 allocations without it. About 0.35 µs of that is logging the headers and payload, which costs no allocations (`BenchmarkMessageLogFields`). About 0.3 µs and 6 allocations come from the context that `Kill` can cancel: a cancelable copy of the broker's context, the `context.AfterFunc` that links it to the server, and the settle timeout, which now registers with a cancelable parent. A hand-written context type could avoid them, but a real message costs at least three inbox queries and a transaction, so none of this is worth optimizing. The default binder also allocates a `json.Decoder` for every message.

Struct layout: `fieldalignment` reports only the ordering of pointer fields, which affects GC scanning, not size. The per-message `State` (48 bytes) and `Message` (112) have no padding to remove. The configs and servers are allocated once.
