# migration: open improvements

Findings from the pre-v1 review of `commands/migration/` that are not applied yet. The applied ones are in REFERENCE.md and the package code.

## Must have

None open.

## Should have

None open.

## Decisions

### `New` keeps `sqlmigrate.MigrationSource`

The v1 API stays tied to sql-migrate's source interface, which also lets users pass any sql-migrate source (memory, embed, packr, bindata). The common cases no longer need it: `CommandConfig.Dir` and `CommandConfig.FS` cover files on disk and `embed`, so only callers of `New` import sql-migrate.

### A transaction does not watch its context

`apply` begins transactions on `context.WithoutCancel`, and its statements run on the deadline-bearing context. When a transaction's context ends, database/sql rolls it back on another goroutine, and the unlock that reuses the session could reach the server before that `ROLLBACK`, failing inside PostgreSQL's aborted transaction. The cost: `Timeout` does not interrupt the `BEGIN` itself.

## Could have

### Checksums

Applied files are assumed immutable, and the docs say so. Storing a hash with each tracking row lets `status` show `modified`, or `up` refuse, when an applied file changed. If this is wanted, adding the column before v1 avoids altering existing tracking tables later.

### Targets and more actions

`-to=<id>` for `up` and `down`, `redo` (down 1, then up 1), and a way to mark migrations as applied without running them, for adopting forgo on an existing database.

### Reject out-of-order migrations

An option to refuse pending migrations that sort before applied ones, as goose does by default. Today they are applied, as REFERENCE.md now says.

### The default MySQL lock name is server-wide

`GET_LOCK('forgo:migrations')` (`defaultMySQLLockName` in `locker.go`) is shared by every database on the server, so unrelated apps on one MySQL server take turns. The docs say to pass your own `NewMySQLLocker` name. Including the database name in the default (hashed to stay within 64 characters) would remove the caveat.

### Smaller items

- The stop notice says "stopping after the running migration" even when no migration is running yet, for example while waiting for the lock. The run then stops at once.
- `-a` and `-action` with different values: the last one wins silently (`command.go`, `Init`).
- A typed `Dialect` with constants instead of the `"mysql"` and `"postgres"` strings. Breaking after v1.
- pgx detection is only checked against the package-path table: the module has no pgx dependency to open a real pgx `*sql.DB` in tests.
