# ForGo migration v1

A reusable Go package with a single action-based migration command. Requires
Go 1.24+. The root package is named `migration`; the command adapter is in
`command.go`.

## Included

- `Migrator.Up`, `Down`, `Status`, and `Plan` for MySQL and PostgreSQL.
- Database advisory locks shared across cooperating processes. The lock, SQL,
  transaction, and tracking queries use **one pinned connection**.
- `Creator.Create` for SQL templates, with exclusive creation and increasing
  UTC millisecond-based versions within a Creator.
- One command with `Init(*flag.FlagSet)`, `Validate() error`, and
  `Run(context.Context) error`, compatible with your ForGo command lifecycle.
- Full SQL statement output in plans, in execution order.
- Tests for both SQL dialects, command validation/output, failure handling,
  lock cleanup, read-only inspection, file creation, and SQL parsing.

## Command surface

```sh
app migrate -a=up
app migrate -action=up -steps=3
app migrate -a=down
app migrate -a=down -steps=3
app migrate -a=status
app migrate -a=plan
app migrate -a=plan -direction=down -steps=2
app migrate -a=new -name=create_users
app migrate -a=new -name=create_users -dir=./migrations
```

The action is required. `up` defaults to all pending migrations; `down` and a
downward `plan` default to one. Explicit `-steps=0` is invalid for downward
operations. Irrelevant flags are rejected even if explicitly set to their
default values, e.g. `-a=status -steps=0`. Positional arguments are rejected.
Go's standard flag parsing rules apply: flags precede positional arguments.

`-a` and `-action` are aliases writing to the same field; if both are passed,
the last one wins, following Go's flag behavior. Create a fresh Command and
FlagSet for each invocation.

## ForGo integration

```go
// Use your real ForGo import paths for cli and these two packages.

cliConf := cli.Config{
    Commands: []cli.CommandConfig{
        {
            Name:    "migrate",
            Command: cli.As(migration.NewCommand),
        },
    },
}
```

Provide the shared services through your bootstrap:

```go
func NewMigrator(db *sql.DB) (*migration.Migrator, error) {
    return migration.New(
        db,
        &sqlmigrate.FileMigrationSource{Dir: "./migrations"},
        migration.NewMySQLLocker("myapp:mydatabase:migrations"),
        migration.Config{
            Dialect: "mysql",
            Table:   "migrations",
            Timeout: 10 * time.Minute,
        },
    )
}

func NewMigrationCreator() *migration.Creator {
    return migration.NewCreator("./migrations")
}

// Add NewMigrator and NewMigrationCreator alongside your DB provider:
// cli.WithProviders(NewMigrationDB, NewMigrator, NewMigrationCreator)
```

For PostgreSQL, change the dialect and locker:

```go
migration.NewPostgresLocker(741234567890)
migration.Config{Dialect: "postgres", Schema: "public", Table: "migrations"}
```

The `Locker` contract differs from the earlier sketch: `Lock(ctx, conn)` gets
the migrator's pinned `*sql.Conn`, so built-in lock constructors take only the
lock name/key. This avoids a second lock connection and works with
`db.SetMaxOpenConns(1)`. Never close the supplied connection inside a custom
locker. All runners targeting the same tracking table must use the same lock
identity. Choose a database/application-specific MySQL name; PostgreSQL keys
are scoped to a database. PostgreSQL requires a direct connection or session
pooling, not a transaction-pooling proxy.

Use `Context: ctx` in your operation context and return command errors from
`Cli.Run()`. Let `main` choose the exit code **after** `Run` returns and cleanup
has completed. The runnable example demonstrates signal cancellation.

The adapter takes an `io.Writer`, so you can choose stdout, a buffer in tests,
or an application output adapter. It preserves write errors and prints the
number of confirmed completed migrations even if a later migration fails.

## SQL format and sources

Files use sql-migrate annotations:

```sql
-- +migrate Up
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
CREATE UNIQUE INDEX idx_users_email ON users(email);

-- +migrate Down
DROP TABLE users;
```

For SQL containing internal semicolons, wrap the statement in
`-- +migrate StatementBegin` and `-- +migrate StatementEnd`. For statements that
must not run in a transaction, use `-- +migrate Up notransaction` (or `Down
notransaction`). Up and down transaction settings are independent.

Any `sqlmigrate.MigrationSource` works, including `FileMigrationSource`,
`MemoryMigrationSource`, and `EmbedFileSystemMigrationSource`:

```go
//go:embed migrations/*.sql
var files embed.FS

source := &sqlmigrate.EmbedFileSystemMigrationSource{
    FileSystem: files,
    Root:       "migrations",
}
```

Keep custom sources immutable or synchronize them. The external source API has
no context parameter: filesystem/custom source loading runs synchronously;
cancellation is checked before and after loading. Every database operation
uses the caller's context. A positive Config.Timeout covers lock acquisition
and database work. Unlock uses a separate five-second cleanup context even if
the caller has cancelled.

## Plan and status behavior

Both operations are read-only: they do not acquire advisory locks, create the
tracking table, or execute migration SQL. A missing tracking table means every
source migration is pending. Plans show migration statements and requested
transaction behavior; the framework's tracking queries are implicit and are
not listed. This is descriptive SQL output, not database `EXPLAIN`.

Example:

```text
Direction: UP
Migrations: 1

[1] 20261004003151237_create_users.sql
    Transaction: requested (MySQL DDL may implicitly commit)

    1. CREATE TABLE users (
           id BIGINT NOT NULL PRIMARY KEY,
           email VARCHAR(255) NOT NULL
       );

    2. CREATE UNIQUE INDEX idx_users_email ON users(email);
```

Plan and execute share the same planner. Up selects unapplied IDs in numeric
prefix/source order and counts earlier pending migrations toward `steps`.
Down selects **only applied IDs**, in reverse source order. A pending earlier
migration is never executed while rolling back. Applied IDs missing from the
source appear as `unknown` in status; plan/up/down fail until the source is
restored. Plans are snapshots: another process can change the database before
an execution, so Up/Down build a fresh plan while holding the lock.

## Tracking and database behavior

Only Up/Down initialize the tracking table. It contains `id` (primary key) and
`applied_at`. Table/schema names are restricted to simple ASCII identifiers of
at most 63 characters and are quoted. Config.Schema must already exist; the
default uses the connection's schema/database. Your application owns the
database pool and must close it; this package never closes the pool.

For the MySQL driver, use **`parseTime=true&loc=UTC`** in the DSN. Drivers are
not registered by the root package; import your chosen driver in the app.

Transactions are per migration, including the tracking insert/delete.
PostgreSQL transactional DDL rolls back with the transaction. MySQL DDL may
implicitly commit; neither this package nor a transaction wrapper can make it
atomic. Nontransactional migrations can also leave partial effects on failure.
An execution error identifies the migration and SQL statement; the returned
count covers confirmed completed migrations only. A commit error may have an
ambiguous outcome: inspect status/schema before retrying. Lock cleanup errors
are returned, and uncertain sessions are physically discarded rather than
returned to the pool with a lock still held. The migration SQL must not change
the session's advisory locks or manage its own transactions when a transaction
is requested.

This v1 uses sql-migrate's SQL parser and sources, with its own small
context-aware planner/executor. It does **not** use `MigrationSet.PlanMigration`
or `ExecMaxContext`: those APIs initialize metadata during planning, use
context-free planning calls, and do not let us pin execution to our lock session.
The tracking column names match sql-migrate, but this is not an automatic
upgrade tool for pre-existing tables: check schema/types/collation before reuse.

There are no checksums or dirty-state records in v1. Treat applied SQL files as
immutable; status cannot detect edits. Empty SQL sections are accepted and
perform tracking updates only. The default SQL parser line-length limit applies
(sql-migrate uses bufio.Scanner); split extremely long SQL lines if necessary.

## Run and verify

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./...

# File creation does not need a database in the standalone example:
go run ./examples/cli migrate -a=new -name=create_users

# PostgreSQL example (supply your own connection string):
export MIGRATION_DIALECT=postgres
export MIGRATION_DSN='postgres://user:password@localhost:5432/app?sslmode=disable'
go run ./examples/cli migrate -a=plan
go run ./examples/cli migrate -a=up
```

In the ForGo factory above, bootstrap resolves the migrator before flags are
parsed, so `-a=new` still resolves the DB provider. The standalone example
parses before resolving database dependencies and avoids that requirement.

The automated unit suite uses database/sql mocks; it verifies SQL flow,
transactions, locks, and cancellation, but does not replace running migrations
on your actual server/driver configuration. An optional integration suite
uses dedicated test databases; see `integration_test.go`.

References:

- [sql-migrate v1.8.1](https://github.com/rubenv/sql-migrate/tree/v1.8.1)
- [MySQL locking functions](https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html)
- [PostgreSQL advisory locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
