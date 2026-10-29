# forgo migration

Package `github.com/efegreat/forgo/commands/migration` provides the `migrate` CLI command for
PostgreSQL and MySQL. It reads [sql-migrate](https://github.com/rubenv/sql-migrate)
files and sources, and runs them under a database advisory lock. See also
[REFERENCE.md](../../REFERENCE.md#migrate-sql-migrations).

## Setup

The default command needs exactly one `*sql.DB` in the bootstrap. It detects
the dialect from the driver (lib/pq or pgx → postgres, go-sql-driver/mysql →
mysql), reads `./migrations` and records applied migrations in the
`migrations` table:

```go
cliConf := cli.Config{
    Commands: []cli.CommandConfig{
        {Name: "migrate", Command: migration.Factory(migration.CommandConfig{})},
    },
}
```

Every field of `CommandConfig` is optional. For example, to embed the files,
pick a named database and rename the tracking table:

```go
//go:embed migrations/*.sql
var files embed.FS

migration.Factory(migration.CommandConfig{
    Database: "primary",        // *sql.DB resource name; default: the only one
    FS:       files,            // default: read Dir from disk
    Dir:      "migrations",     // in FS or on disk; "new" writes here on disk
    Config: migration.Config{
        Dialect: migration.Postgres,   // default: detected from the driver
        Schema:  "public",             // optional, must already exist
        Table:   "schema_migrations",  // default "migrations"
        Timeout: 10 * time.Minute,     // whole run, including the lock wait; 0 = none
    },
    Locker: migration.NewPostgresLocker(741234567890), // or migration.NewMySQLLocker("myapp:migrations")
})
```

Set `Dialect` when the driver is wrapped, for example for tracing, or is not
one of the three above. Every runner that targets the same tracking table must
use the same lock; the default lock is shared by every forgo migrator.

Outside the CLI, `migration.New(db, source, nil, migration.Config{})` returns a
`*Migrator` with `Up`, `Down`, `Plan` and `Status`, with the same defaults.

## Usage

```sh
app migrate -a=up                                # apply all pending migrations
app migrate -a=up -steps=3                       # apply the next 3
app migrate -a=down                              # roll back the last applied migration
app migrate -a=down -steps=3                     # roll back 3
app migrate -a=status                            # ID / STATE / APPLIED AT table
app migrate -a=plan                              # print the SQL that "up" would run
app migrate -a=plan -direction=down -steps=2
app migrate -a=new -name=create_users            # create a migration template
app migrate -a=new -name=create_users -dir=./db/migrations
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `-a`, `-action` | all | `up`, `down`, `status`, `plan`, `new` (required) |
| `-steps` | `up`, `down`, `plan` | Up: `0` (default) means all. Down: defaults to 1; an explicit value must be positive. |
| `-direction` | `plan` | `up` (default) or `down` |
| `-name` | `new` | Letters, digits and underscores, starting with a letter |
| `-dir` | `new` | Override the target directory |

Flags that don't apply to the chosen action are rejected, as are positional
arguments.

`up` and `down` print each migration as it runs:

```text
applying 20261004003151237_create_users.sql
applied 20261004003151237_create_users.sql (42ms)
up: 1 migration completed
```

`plan` prints the statements of each migration in execution order:

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

## Migration files

`new` creates `<yyyyMMddHHmmssSSS>_<name>.sql` (UTC). Files use sql-migrate
annotations:

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

- Wrap statements that contain semicolons in `-- +migrate StatementBegin` and
  `-- +migrate StatementEnd`.
- Use `-- +migrate Up notransaction` (or `Down notransaction`) for statements
  that cannot run inside a transaction.
- Treat applied files as immutable. Each tracking row holds a SHA-256 of the
  migration's Up statements, so an edited Up section shows as `modified` in
  `status` and blocks `up`. Edits to whole-line `-- ` comments and to the Down
  section do not count.

## Behavior

- `up` and `down` hold the advisory lock on one pinned connection. Migration
  SQL, tracking writes and the lock share that session. While another run
  holds the lock, they print `waiting for the migration lock` and keep trying,
  up to `Timeout`. The tracking table is created on first use.
- Each migration runs in its own transaction together with its tracking row,
  unless it is marked `notransaction`.
- `up` applies pending migrations in source order, including one that sorts
  before applied ones, such as a migration merged later from a branch.
- `down` only rolls back applied migrations, in reverse source order, not in
  reverse order of application.
- `status` and `plan` are read-only: no lock, no table creation, no SQL
  execution.
- Applied migrations missing from the source show as `unknown` in `status` and
  block `up`, `down` and `plan` until the source is restored.
- Applied migrations whose Up statements changed show as `modified` in
  `status` and block `up` and `plan` of `up` until the file is restored, or
  until `down` rolls the migration back with its current Down statements and
  `up` applies it again.
- On failure, the error names the migration and statement number. Migrations
  completed before it stay applied and are reported.
- The first SIGINT or SIGTERM stops `up` and `down` after the running
  migration, which completes with its tracking row. A second signal kills the
  process. `Timeout` interrupts the running migration itself.
- The CLI builds every resource before parsing flags, so `-a=new` still opens
  the database.

## Database notes

- **MySQL:** `parseTime=true` is not required, but keep `loc` at its default,
  UTC. DDL commits implicitly, so a failed migration can leave partial
  changes. The default lock is server-wide: give each app its own
  `NewMySQLLocker` name if several databases share a server.
- **PostgreSQL:** advisory locks need a direct connection or session pooling,
  not a transaction-pooling proxy.
- The package imports no driver: import yours in your application. It never
  closes your `*sql.DB`.
- After a commit error the outcome is uncertain. Check `status` before
  retrying.

## Testing

```sh
go test -race ./commands/migration

# Integration suite, against disposable databases only:
MIGRATION_TEST_POSTGRES_DSN='postgres://user:password@localhost:5432/test?sslmode=disable' \
MIGRATION_TEST_MYSQL_DSN='user:password@tcp(localhost:3306)/test?parseTime=true&loc=UTC' \
go test -run TestIntegration -v ./commands/migration
```
