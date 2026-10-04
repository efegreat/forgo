// Package migration runs SQL migrations on MySQL and PostgreSQL. It accepts
// sql-migrate sources and SQL annotations, while keeping execution, tracking,
// planning, and advisory locking on context-aware database/sql APIs.
package migration

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Direction uint8

const (
	Up Direction = iota
	Down
)

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

type StatusState uint8

const (
	StatusPending StatusState = iota
	StatusApplied
	StatusUnknown
)

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

type Status struct {
	ID        string
	State     StatusState
	AppliedAt *time.Time
}

type PlannedMigration struct {
	ID          string
	Direction   Direction
	Transaction bool // A transaction is requested; MySQL DDL may implicitly commit.
	Statements  []string
}

type Config struct {
	Dialect string        // "mysql" or "postgres"
	Table   string        // Defaults to "migrations".
	Schema  string        // Must already exist; defaults to the connection's schema/database.
	Timeout time.Duration // Total time for an operation, including lock acquisition; 0 disables.
}

type UnlockFunc func(context.Context) error

// Locker must acquire and release its lock on conn without closing conn.
// The migrator owns conn and executes all migration SQL on that same session.
// An acquisition error must not return a session with an uncertain held lock:
// implementations must discard that connection if cleanup cannot be confirmed.
type Locker interface {
	Lock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error)
}

// ExecutionError identifies an incomplete migration. Count returned by Up/Down
// includes only migrations whose SQL and tracking update finished successfully.
// Nontransactional SQL and MySQL DDL may have taken effect before this error.
type ExecutionError struct {
	ID        string
	Direction Direction
	Statement int // 1-based SQL statement; 0 means transaction/tracking/commit.
	Err       error
}

func (e *ExecutionError) Error() string {
	if e.Statement > 0 {
		return fmt.Sprintf("migration %s %q statement %d: %v", e.Direction, e.ID, e.Statement, e.Err)
	}
	return fmt.Sprintf("migration %s %q: %v", e.Direction, e.ID, e.Err)
}

func (e *ExecutionError) Unwrap() error { return e.Err }
