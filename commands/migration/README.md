# forgo migration

Package `forgo/commands/migration` provides the `migrate` CLI command for
PostgreSQL and MySQL. It reads [sql-migrate](https://github.com/rubenv/sql-migrate)
files and sources, and runs them under a database advisory lock. See also
[REFERENCE.md](../../REFERENCE.md#migrate-sql-migrations).

## Setup

The default command needs exactly one `*sql.DB` in the bootstrap. It detects
the dialect from the driver, reads `./migrations` and records applied
migrations in the `migrations` table:

```go
cliConf := cli.Config{
    Commands: []cli.CommandConfig{
        {Name: "migrate", Command: cli.As(migration.NewDefaultMigrateCommand)},
    },
}
```

For another source, table, schema, lock or timeout, write your own constructor:

```go
func NewMigrateCommand(db *sql.DB) (cli.CommandFactory, error) {
    m, err := migration.New(
        db,
        &sqlmigrate.FileMigrationSource{Dir: "./migrations"},
        migration.NewPostgresLocker(741234567890), // or migration.NewMySQLLocker("myapp:migrations")
        migration.Config{
            Dialect: "postgres",         // "postgres" | "mysql"
            Schema:  "public",           // optional, must already exist
            Table:   "migrations",       // default "migrations"
            Timeout: 10 * time.Minute,   // whole operation, including the lock wait; 0 = none
        },
    )
    if err != nil {
        return nil, err
    }
    creator := migration.NewCreator("./migrations")
    return func() cli.Command { return migration.NewCommand(m, creator, os.Stdout) }, nil
}

// {Name: "migrate", Command: cli.As(NewMigrateCommand)}
```

Any `sqlmigrate.MigrationSource` works, for example an embedded directory:

```go
//go:embed migrations/*.sql
var files embed.FS

source := &sqlmigrate.EmbedFileSystemMigrationSource{FileSystem: files, Root: "migrations"}
```

Every runner that targets the same tracking table must use the same lock
name (MySQL) or key (PostgreSQL).

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
- Treat applied files as immutable. There are no checksums, so edits go
  unnoticed.

## Behavior

- `up` and `down` hold the advisory lock on one pinned connection. Migration
  SQL, tracking writes and the lock share that session. The tracking table is
  created on first use.
- Each migration runs in its own transaction together with its tracking row,
  unless it is marked `notransaction`.
- `down` only rolls back applied migrations, in reverse order.
- `status` and `plan` are read-only: no lock, no table creation, no SQL
  execution.
- Applied migrations missing from the source show as `unknown` in `status` and
  block `up`, `down` and `plan` until the source is restored.
- On failure, the error names the migration and statement number. Migrations
  completed before it stay applied and are reported.
- The command stops on SIGINT or SIGTERM through the CLI context.
- The CLI builds every resource before parsing flags, so `-a=new` still opens
  the database.

## Database notes

- **MySQL:** the DSN needs `parseTime=true` (`loc=UTC` recommended). DDL
  commits implicitly, so a failed migration can leave partial changes.
- **PostgreSQL:** advisory locks need a direct connection or session pooling,
  not a transaction-pooling proxy.
- Import the database driver in your application. The package never closes
  your `*sql.DB`.
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
