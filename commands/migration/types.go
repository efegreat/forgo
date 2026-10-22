// Package migration runs SQL migrations on PostgreSQL and MySQL, and provides the migrate
// command of the forgo CLI.
//
// Migrator plans, applies and rolls back the migrations of a sql-migrate source under an
// advisory lock, and records them in a tracking table. Command runs a Migrator from the command
// line, Factory builds that command from a CommandConfig, and Creator writes new migration
// files. MySQLLocker and PostgresLocker are the advisory locks.
//
// See the [SQL migrations] section of REFERENCE.md.
//
// [SQL migrations]: https://github.com/efegreat/forgo/blob/main/REFERENCE.md#migrate-sql-migrations
package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrUnsupportedDialect is returned when Config.Dialect is neither "mysql" nor "postgres", or
	// when it is empty and the database driver is not one DetectDialect knows.
	ErrUnsupportedDialect = errors.New("migration: unsupported dialect")
	// ErrUnknownMigrations is returned by Up, Down and Plan when the tracking table holds
	// migrations that the source does not have. Restore them in the source to migrate again.
	ErrUnknownMigrations = errors.New("migration: applied migrations missing from source")
)

// Direction is the way a migration runs.
type Direction uint8

const (
	// Up applies migrations.
	Up Direction = iota
	// Down rolls migrations back.
	Down
)

// String returns "up" or "down".
func (d Direction) String() string {
	switch d {
	case Up:
		return "up"
	case Down:
		return "down"
	default:
		return fmt.Sprintf("Direction(%d)", d)
	}
}

// StatusState is the state of a migration in Migrator.Status.
type StatusState uint8

const (
	// StatusPending is a migration of the source that is not applied.
	StatusPending StatusState = iota
	// StatusApplied is a migration of the source that is applied.
	StatusApplied
	// StatusUnknown is an applied migration that the source does not have. It blocks Up, Down
	// and Plan until the source has it again.
	StatusUnknown
)

// String returns "pending", "applied" or "unknown".
func (s StatusState) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusApplied:
		return "applied"
	case StatusUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("StatusState(%d)", s)
	}
}

// Status is the state of one migration.
type Status struct {
	ID        string
	State     StatusState
	AppliedAt *time.Time // nil while pending
}

// PlannedMigration is a migration that Up or Down runs, or that Plan reports, with the
// statements of its direction.
type PlannedMigration struct {
	ID          string
	Direction   Direction
	Transaction bool // A transaction is requested; MySQL DDL may implicitly commit.
	Statements  []string
}

// Config configures a Migrator. Its zero value detects the dialect from the database driver and
// tracks migrations in the table "migrations" of the connection's schema, without a time limit.
type Config struct {
	// Dialect is "mysql" or "postgres". When empty, New detects it with DetectDialect.
	Dialect string
	// Table is the tracking table, created by the first Up or Down. Defaults to "migrations".
	Table string
	// Schema holds the tracking table and must already exist. Defaults to the connection's
	// schema (PostgreSQL) or database (MySQL).
	Schema string
	// Timeout bounds each Up, Down, Plan and Status call, including the wait for the lock.
	// Unlike a cancellation, it also interrupts a running migration. Zero means no limit.
	Timeout time.Duration
}

// UnlockFunc releases a lock taken by Locker.TryLock. The Migrator calls it once, with a context
// that is not cancelled, and discards the session if it fails.
type UnlockFunc func(context.Context) error

// Locker takes the advisory lock that keeps migrators from running at the same time. Unlike
// operation.ILocker, the lock belongs to a database session: the Migrator takes it on the
// connection that runs the migrations, so it also ends when the process dies.
//
// Every Migrator that targets the same tracking table must use the same lock.
type Locker interface {
	// TryLock makes one attempt to take the lock on conn, and must not close conn. While another
	// session holds the lock, it returns a nil UnlockFunc and a nil error, and the Migrator tries
	// again. After an error, the Migrator discards the session, because the server may have
	// taken the lock even if its response was lost.
	TryLock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error)
}

// ExecutionError identifies the migration that failed. The count returned by Up and Down only
// includes migrations whose SQL and tracking row completed. Nontransactional SQL and MySQL DDL
// may have taken effect before this error.
type ExecutionError struct {
	ID        string
	Direction Direction
	Statement int // 1-based SQL statement; 0 means transaction/tracking/commit.
	Err       error
}

// Error names the direction, the migration and, when known, the statement that failed.
func (e *ExecutionError) Error() string {
	if e.Statement > 0 {
		return fmt.Sprintf("migration: %s %q statement %d: %v", e.Direction, e.ID, e.Statement, e.Err)
	}
	return fmt.Sprintf("migration: %s %q: %v", e.Direction, e.ID, e.Err)
}

// Unwrap returns Err.
func (e *ExecutionError) Unwrap() error { return e.Err }

// Progress receives the progress of Migrator.Up and Migrator.Down, on the goroutine that called
// them. Nil fields are skipped. Attach it to the context with WithProgress.
type Progress struct {
	// LockWait is called once when another session holds the lock. The Migrator keeps trying
	// until it gets the lock or the context ends.
	LockWait func()
	// Start is called before a migration runs.
	Start func(PlannedMigration)
	// Done is called after a migration and its tracking row completed.
	Done func(PlannedMigration, time.Duration)
}

type progressKey struct{}

// WithProgress returns a copy of ctx that makes Up and Down report to p. Command uses it to
// print each migration, so it works with any Runner that honors it.
func WithProgress(ctx context.Context, p Progress) context.Context {
	return context.WithValue(ctx, progressKey{}, p)
}

func progressFrom(ctx context.Context) Progress {
	p, _ := ctx.Value(progressKey{}).(Progress)
	return p
}
