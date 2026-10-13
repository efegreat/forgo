# server/cron: improvements

Findings from the pre-v1 review of `server/cron/`.

## Must have

None.

## Should have

None.

## Could have

### `Server` is exported

`Config.New` returns a `forgo.IServer`, and nothing needs `*cron.Server`. Unexporting it shrinks the API, but `http.Server`, `worker.ConsumerServer` and `worker.ProducerServer` are exported the same way, so decide it for every server at once.

### `JobFor`, like `http.HandlerFor`

`IJob` only has `Run`, so `cron.JobFor((*cart.Service).ExpireStale)` could resolve the receiver from the bootstrap and run the method. A service method would become a job without a job type, as `http.HandlerFor` and `worker.HandlerFor` do for handlers.

### Seconds and a default time zone

The parser takes the standard 5 fields. `@every` covers sub-minute intervals, but not "at second 30". A `CRON_TZ=` prefix sets a job's zone, but a server-wide `Config.Location` would save repeating it. robfig supports both, through `cron.WithSeconds` and `cron.WithLocation`.

### Check the lock TTL against the tolerance

REFERENCE.md says a job's `Lock.TTL` must cover `RetryAttempts × (run time + RetryDelay)`. Both are in `JobConfig`, so `New` could reject a TTL shorter than `(RetryAttempts - 1) × RetryDelay`, which lets the lock expire during the retries of a failing run whatever its run time, and a lock combined with unlimited attempts.

### Log the next run

`cron job added` logs the schedule, but not when the job runs next. A `next_run` field, from `Schedule.Next(time.Now())`, would show at a glance whether a `CRON_TZ` or a day-of-week field means what its author thought.

### A job timeout

A job has no timeout of its own, apart from `Tolerance.RetryTimeout` when it retries. The transport-agnostic `middleware.Timeout` proposed in `middleware/improvement.md`, attached through `JobConfig.Middlewares`, would cover it.

## Performance

Measured with `go test ./server/cron -bench Run` on an Apple M5. A run of a job that does nothing costs about 575 ns and 7 allocations (880 B) with the base chain, almost all of it the chain itself (see `middleware/improvement.md`), and 25 ns and 2 allocations (the `operation.Context` and its `Args`) without it. A job runs at most once a second, so neither matters next to the job.

`Server` (192 bytes), `Config` (120), `JobConfig` (152) and the resolved job (176) have no padding to remove, apart from one `bool` in `Server` that no reordering can share. They are allocated once per server or job. Nothing to repack.
