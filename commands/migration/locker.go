package migration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"unicode/utf8"
)

// The default lock, shared by every Migrator built with a nil Locker.
const (
	defaultMySQLLockName   = "forgo:migrations"
	defaultPostgresLockKey = 0x466F72476F // "ForGo"
)

func defaultLocker(dialect string) Locker {
	if dialect == "mysql" {
		return NewMySQLLocker(defaultMySQLLockName)
	}
	return NewPostgresLocker(defaultPostgresLockKey)
}

// MySQLLocker is a Locker built on MySQL's GET_LOCK. Its lock is server-wide, not per database.
type MySQLLocker struct{ name string }

// NewMySQLLocker returns a MySQLLocker for the lock name, at most 64 characters. Because the lock
// is server-wide, include your database or application in the name, for example
// "shop:migrations". Every competing runner must use the same name.
func NewMySQLLocker(name string) *MySQLLocker { return &MySQLLocker{name: name} }

// TryLock implements Locker with GET_LOCK(name, 0).
func (l *MySQLLocker) TryLock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error) {
	if l == nil || strings.TrimSpace(l.name) == "" || !utf8.ValidString(l.name) || utf8.RuneCountInString(l.name) > 64 {
		return nil, errors.New("migration: MySQL lock name must contain 1 to 64 characters")
	}
	var result sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", l.name).Scan(&result); err != nil {
		return nil, err
	}
	if !result.Valid || (result.Int64 != 0 && result.Int64 != 1) {
		return nil, errors.New("migration: GET_LOCK returned an invalid result")
	}
	if result.Int64 == 0 {
		return nil, nil
	}
	return func(ctx context.Context) error {
		var released sql.NullInt64
		if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", l.name).Scan(&released); err != nil {
			return err
		}
		if !released.Valid || released.Int64 != 1 {
			return errors.New("migration: MySQL lock was not held by this session")
		}
		return nil
	}, nil
}

// PostgresLocker is a Locker built on PostgreSQL session advisory locks, which are per database.
type PostgresLocker struct{ key int64 }

// NewPostgresLocker returns a PostgresLocker for the advisory lock key. Use the same stable key
// for every runner targeting the same tracking table. Session locks need direct connections or
// session pooling: a transaction-pooling proxy cannot keep them.
func NewPostgresLocker(key int64) *PostgresLocker { return &PostgresLocker{key: key} }

// TryLock implements Locker with pg_try_advisory_lock(key).
func (l *PostgresLocker) TryLock(ctx context.Context, conn *sql.Conn) (UnlockFunc, error) {
	if l == nil {
		return nil, errors.New("migration: PostgreSQL locker is nil")
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", l.key).Scan(&acquired); err != nil {
		return nil, err
	}
	if !acquired {
		return nil, nil
	}
	return func(ctx context.Context) error {
		var released bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", l.key).Scan(&released); err != nil {
			return err
		}
		if !released {
			return errors.New("migration: PostgreSQL lock was not held by this session")
		}
		return nil
	}, nil
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
