package migration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const lockPollInterval = 100 * time.Millisecond

type MySQLLocker struct{ name string }

// NewMySQLLocker uses a server-wide lock name. Include your database/application
// name in it, e.g. "service:migrations"; all competing runners must use that name.
func NewMySQLLocker(name string) *MySQLLocker { return &MySQLLocker{name: name} }

func (l *MySQLLocker) Lock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error) {
	if l == nil || strings.TrimSpace(l.name) == "" || !utf8.ValidString(l.name) || utf8.RuneCountInString(l.name) > 64 {
		return nil, errors.New("MySQL lock name must contain 1 to 64 characters")
	}
	return acquireLock(ctx, conn,
		func(ctx context.Context) (bool, error) {
			var result sql.NullInt64
			err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", l.name).Scan(&result)
			if err != nil {
				return false, err
			}
			if !result.Valid || (result.Int64 != 0 && result.Int64 != 1) {
				return false, errors.New("GET_LOCK returned an invalid result")
			}
			return result.Int64 == 1, nil
		},
		func(ctx context.Context) error {
			var result sql.NullInt64
			if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", l.name).Scan(&result); err != nil {
				return err
			}
			if !result.Valid || result.Int64 != 1 {
				return errors.New("MySQL migration lock was not held by this session")
			}
			return nil
		})
}

type PostgresLocker struct{ key int64 }

// NewPostgresLocker uses a database-local session advisory lock. Use the same
// stable key for every runner targeting this migration table. Requires direct
// connections or session pooling; transaction pooling cannot retain this lock.
func NewPostgresLocker(key int64) *PostgresLocker { return &PostgresLocker{key: key} }

func (l *PostgresLocker) Lock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error) {
	if l == nil {
		return nil, errors.New("PostgreSQL locker is nil")
	}
	return acquireLock(ctx, conn,
		func(ctx context.Context) (bool, error) {
			var acquired bool
			err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", l.key).Scan(&acquired)
			return acquired, err
		},
		func(ctx context.Context) error {
			var released bool
			if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", l.key).Scan(&released); err != nil {
				return err
			}
			if !released {
				return errors.New("PostgreSQL migration lock was not held by this session")
			}
			return nil
		})
}

func acquireLock(ctx context.Context, conn *sql.Conn,
	try func(context.Context) (bool, error), release func(context.Context) error,
) (UnlockFunc, error) {
	if conn == nil {
		return nil, errors.New("migration connection is nil")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		acquired, err := try(ctx)
		if err != nil {
			// The server may have acquired the lock even if its response was lost.
			// Conn.Close alone would return the session (and lock) to the pool.
			_ = discardConnection(conn)
			return nil, fmt.Errorf("acquire advisory lock: %w", err)
		}
		if acquired {
			var once sync.Once
			var releaseErr error
			return func(cleanupCtx context.Context) error {
				once.Do(func() {
					releaseErr = release(cleanupCtx)
					if releaseErr != nil {
						_ = discardConnection(conn)
					}
				})
				return releaseErr
			}, nil
		}
		timer := time.NewTimer(lockPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Returning ErrBadConn from Raw makes database/sql close the physical session,
// rather than put a potentially locked session back in the connection pool.
func discardConnection(conn *sql.Conn) error {
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return nil
	}
	return err
}

var _ Locker = (*MySQLLocker)(nil)
var _ Locker = (*PostgresLocker)(nil)
