package migration

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func testConn(t *testing.T, m *Migrator) *sql.Conn {
	t.Helper()
	conn, err := m.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func expectBusy(mock sqlmock.Sqlmock, dialect Dialect) {
	if dialect == MySQL {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(0))
	} else {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT pg_try_advisory_lock($1)")).WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(false))
	}
}

// alive reports whether conn still holds its session.
func alive(conn *sql.Conn) bool {
	return conn.Raw(func(any) error { return nil }) == nil
}

func TestLockWaitsAndReportsOnce(t *testing.T) {
	for _, dialect := range []Dialect{MySQL, Postgres} {
		t.Run(string(dialect), func(t *testing.T) {
			m, mock := testMigrator(t, dialect, source())
			conn := testConn(t, m)
			expectBusy(mock, dialect)
			expectBusy(mock, dialect)
			expectLock(mock, dialect)
			waits := 0
			unlock, err := m.lock(context.Background(), conn, Progress{LockWait: func() { waits++ }})
			if err != nil || unlock == nil || waits != 1 {
				t.Fatalf("unlock=%v, waits=%d, err=%v", unlock != nil, waits, err)
			}
			assertExpectations(t, mock)
		})
	}
}

func TestLockWaitCancellationKeepsSession(t *testing.T) {
	for _, dialect := range []Dialect{MySQL, Postgres} {
		t.Run(string(dialect), func(t *testing.T) {
			m, mock := testMigrator(t, dialect, source())
			conn := testConn(t, m)
			expectBusy(mock, dialect)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			unlock, err := m.lock(ctx, conn, Progress{})
			if unlock != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected lock result: %v", err)
			}
			if !alive(conn) {
				t.Fatal("a cleanly refused lock discarded the session")
			}
			assertExpectations(t, mock)
		})
	}
}

func TestAmbiguousAcquireDiscardsSession(t *testing.T) {
	m, mock := testMigrator(t, "postgres", source())
	conn := testConn(t, m)
	failed := errors.New("response lost")
	mock.ExpectQuery("SELECT pg_try_advisory_lock").WillReturnError(failed)
	mock.ExpectClose()
	if _, err := m.lock(context.Background(), conn, Progress{}); !errors.Is(err, failed) {
		t.Fatalf("err=%v", err)
	}
	if alive(conn) {
		t.Fatal("session kept after an ambiguous acquire")
	}
	assertExpectations(t, mock)
}

func TestMySQLNullResultDiscardsSession(t *testing.T) {
	m, mock := testMigrator(t, "mysql", source())
	conn := testConn(t, m)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(nil))
	mock.ExpectClose()
	if _, err := m.lock(context.Background(), conn, Progress{}); err == nil {
		t.Fatal("expected NULL result failure")
	}
	assertExpectations(t, mock)
}

func TestReleaseOfLockNotHeldFails(t *testing.T) {
	for name, release := range map[string]any{"mysql": 0, "mysql-null": nil, "postgres": false} {
		t.Run(name, func(t *testing.T) {
			dialect := Dialect(strings.TrimSuffix(name, "-null"))
			m, mock := testMigrator(t, dialect, source())
			conn := testConn(t, m)
			expectLock(mock, dialect)
			mock.ExpectQuery("SELECT (RELEASE_LOCK|pg_advisory_unlock)").WillReturnRows(sqlmock.NewRows([]string{"unlock"}).AddRow(release))
			unlock, err := m.locker.TryLock(context.Background(), conn)
			if err != nil {
				t.Fatal(err)
			}
			if err := unlock(context.Background()); err == nil || !strings.Contains(err.Error(), "not held") {
				t.Fatalf("err=%v", err)
			}
			assertExpectations(t, mock)
		})
	}
}

func TestMySQLLockNameValidation(t *testing.T) {
	m, mock := testMigrator(t, "mysql", source())
	conn := testConn(t, m)
	for _, name := range []string{"", "   ", strings.Repeat("a", 65), "\xff"} {
		if _, err := NewMySQLLocker(name).TryLock(context.Background(), conn); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	// The limit counts characters, not bytes.
	name := strings.Repeat("ü", 64)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT GET_LOCK(?, 0)")).WithArgs(name).WillReturnRows(sqlmock.NewRows([]string{"lock"}).AddRow(1))
	if unlock, err := NewMySQLLocker(name).TryLock(context.Background(), conn); err != nil || unlock == nil {
		t.Fatalf("rejected a 64-character name: %v", err)
	}
	assertExpectations(t, mock)
}

func TestNilLockersFail(t *testing.T) {
	for _, locker := range []Locker{(*MySQLLocker)(nil), (*PostgresLocker)(nil)} {
		if _, err := locker.TryLock(context.Background(), nil); err == nil {
			t.Fatalf("%T accepted", locker)
		}
	}
}
